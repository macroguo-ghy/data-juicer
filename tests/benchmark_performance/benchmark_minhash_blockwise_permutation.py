#!/usr/bin/env python3
"""Fresh-process A/B benchmark for blockwise MinHash permutations."""

import argparse
import hashlib
import json
import os
import platform
import random
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

from data_juicer.ops.common.helper_func import split_on_whitespace
from data_juicer.ops.deduplicator.document_minhash_deduplicator import (
    MAX_HASH,
    MERSENNE_PRIME,
    DocumentMinhashDeduplicator,
    sha1_hash32,
)
from data_juicer.utils.constant import HashKeys

_SCRIPT_PATH = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT_PATH.parents[2]
_VARIANTS = ("baseline", "blockwise")
_RESULT_PREFIX = "MINHASH_BENCHMARK_RESULT="


class _EagerDocumentMinhashDeduplicator(DocumentMinhashDeduplicator):
    def compute_hash(self, sample):
        if HashKeys.minhash in sample:
            return sample

        text = sample[self.text_key]
        if self.lowercase:
            text = text.lower()
        if self.ignore_pattern:
            text = self.ignore_pattern.sub("", text)

        if self.tokenization == "character":
            tokens = {str.encode(text[i : i + self.window_size]) for i in range(len(text) - self.window_size + 1)}
        elif self.tokenization == "punctuation":
            tokens = self.punctuation_pattern.split(text)
            tokens = {
                str.encode(" ".join(tokens[i : i + self.window_size]))
                for i in range(len(tokens) - self.window_size + 1)
            }
        elif self.tokenization == "space":
            tokens = split_on_whitespace(text)
            tokens = {
                str.encode(" ".join(tokens[i : i + self.window_size]))
                for i in range(len(tokens) - self.window_size + 1)
            }
        elif self.tokenization == "sentencepiece":
            tokens = self.tokenizer.encode(text, out_type=str)
            tokens = {
                str.encode("".join(tokens[i : i + self.window_size])) for i in range(len(tokens) - self.window_size + 1)
            }
        else:
            raise NotImplementedError(f"Unimplemented tokenization method [{self.tokenization}]")

        hv = np.fromiter((sha1_hash32(token) for token in tokens), dtype=np.uint64, count=len(tokens))
        phv = np.bitwise_and((hv[:, None] * self.perm_a + self.perm_b) % MERSENNE_PRIME, MAX_HASH)
        hash_values = phv.min(axis=0)
        sample[HashKeys.minhash] = [bytes(hash_values[start:end].byteswap().data) for start, end in self.hash_ranges]
        return sample


def _new_op(variant, num_permutations):
    cls = _EagerDocumentMinhashDeduplicator if variant == "baseline" else DocumentMinhashDeduplicator
    return cls(
        tokenization="space",
        window_size=1,
        num_permutations=num_permutations,
        num_bands=32,
        num_rows_per_band=num_permutations // 32,
        auto_op_parallelism=False,
        num_proc=1,
    )


def _token_text(count):
    return " ".join(f"token-{index:06d}" for index in range(count))


def _signature(result):
    bands = result[HashKeys.minhash]
    if len(bands) != 32:
        raise AssertionError(f"unexpected band count: {len(bands)}")
    return hashlib.sha256(b"".join(bands)).hexdigest()


def _max_rss_bytes():
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(max_rss if sys.platform == "darwin" else max_rss * 1024)


def _memory_worker(args):
    op = _new_op(args.variant, args.num_permutations)
    text = _token_text(args.tokens)
    result = op.compute_hash({"text": text, "ordinal": 17})
    print(
        _RESULT_PREFIX
        + json.dumps(
            {
                "max_rss_bytes": _max_rss_bytes(),
                "signature": _signature(result),
                "ordinal": result["ordinal"],
            }
        )
    )


def _latency_worker(args):
    op = _new_op(args.variant, args.num_permutations)
    text = _token_text(args.tokens)

    def run_once():
        return op.compute_hash({"text": text, "ordinal": 17})

    for _ in range(args.warmup_calls):
        run_once()

    observations = []
    signature = None
    for _ in range(args.latency_observations):
        started = time.perf_counter()
        result = run_once()
        observations.append(time.perf_counter() - started)
        current_signature = _signature(result)
        if signature is None:
            signature = current_signature
        elif signature != current_signature:
            raise AssertionError("MinHash signature changed between observations")

    print(_RESULT_PREFIX + json.dumps({"seconds_per_call": observations, "signature": signature}))


def _run_worker(arguments):
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(_REPO_ROOT) if not current_pythonpath else f"{_REPO_ROOT}{os.pathsep}{current_pythonpath}"
    completed = subprocess.run(
        [sys.executable, str(_SCRIPT_PATH), *arguments],
        cwd=_REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"worker failed ({' '.join(arguments)}):\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    records = [
        line.removeprefix(_RESULT_PREFIX) for line in completed.stdout.splitlines() if line.startswith(_RESULT_PREFIX)
    ]
    if len(records) != 1:
        raise RuntimeError(f"worker emitted {len(records)} result records; expected one")
    return json.loads(records[0])


def _percentile(values, percentile):
    ordered = sorted(values)
    index = max(0, int(np.ceil(len(ordered) * percentile)) - 1)
    return ordered[index]


def _linear_slope(xs, ys):
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denominator


def _summarize_memory(samples, tokens_per_scale, scales):
    by_scale = {}
    medians = {variant: [] for variant in _VARIANTS}
    token_counts = [tokens_per_scale * scale for scale in scales]
    for scale in scales:
        scale_key = str(scale)
        baseline = statistics.median(samples["baseline"][scale_key])
        blockwise = statistics.median(samples["blockwise"][scale_key])
        medians["baseline"].append(baseline)
        medians["blockwise"].append(blockwise)
        by_scale[scale_key] = {
            "tokens": tokens_per_scale * scale,
            "baseline_median_rss_bytes": baseline,
            "blockwise_median_rss_bytes": blockwise,
            "rss_reduction": 1 - blockwise / baseline,
        }
    baseline_slope = _linear_slope(token_counts, medians["baseline"])
    blockwise_slope = _linear_slope(token_counts, medians["blockwise"])
    slope_reduction = None if baseline_slope <= 0 or blockwise_slope < 0 else 1 - blockwise_slope / baseline_slope
    return {
        "by_scale": by_scale,
        "rss_growth": {
            "baseline_bytes_per_token": baseline_slope,
            "blockwise_bytes_per_token": blockwise_slope,
            "slope_reduction": slope_reduction,
        },
    }


def _summarize_latency(samples):
    baseline = samples["baseline"]
    blockwise = samples["blockwise"]
    baseline_median = statistics.median(baseline)
    blockwise_median = statistics.median(blockwise)
    baseline_p95 = _percentile(baseline, 0.95)
    blockwise_p95 = _percentile(blockwise, 0.95)
    return {
        "baseline_median_seconds": baseline_median,
        "blockwise_median_seconds": blockwise_median,
        "throughput_ratio": baseline_median / blockwise_median,
        "baseline_p95_seconds": baseline_p95,
        "blockwise_p95_seconds": blockwise_p95,
        "p95_latency_ratio": blockwise_p95 / baseline_p95,
    }


def _print_markdown(result):
    print("## Peak RSS")
    print()
    print("| Scale | Unique tokens | Eager MiB | Blockwise MiB | Reduction |")
    print("| ---: | ---: | ---: | ---: | ---: |")
    for scale, values in result["memory"]["summary"]["by_scale"].items():
        print(
            f"| {scale}x | {values['tokens']:,} | "
            f"{values['baseline_median_rss_bytes'] / 1024**2:.1f} | "
            f"{values['blockwise_median_rss_bytes'] / 1024**2:.1f} | "
            f"{values['rss_reduction']:.1%} |"
        )
    growth = result["memory"]["summary"]["rss_growth"]
    slope_reduction = "n/a" if growth["slope_reduction"] is None else f"{growth['slope_reduction']:.1%} reduction"
    print()
    print(
        f"RSS slope: {growth['baseline_bytes_per_token']:.2f} -> "
        f"{growth['blockwise_bytes_per_token']:.2f} B/token "
        f"({slope_reduction})"
    )
    latency = result["latency"]["summary"]
    print()
    print("## Latency")
    print()
    print("| Eager median ms | Blockwise median ms | Throughput ratio | Blockwise/eager P95 |")
    print("| ---: | ---: | ---: | ---: |")
    print(
        f"| {latency['baseline_median_seconds'] * 1000:.3f} | "
        f"{latency['blockwise_median_seconds'] * 1000:.3f} | "
        f"{latency['throughput_ratio']:.3f}x | {latency['p95_latency_ratio']:.3f}x |"
    )


def _benchmark(args):
    memory_samples = {variant: {str(scale): [] for scale in args.scales} for variant in _VARIANTS}
    signatures = {variant: {str(scale): set() for scale in args.scales} for variant in _VARIANTS}
    tasks = [
        (variant, scale, repetition)
        for variant in _VARIANTS
        for scale in args.scales
        for repetition in range(args.memory_repetitions)
    ]
    random.Random(args.seed).shuffle(tasks)
    for index, (variant, scale, repetition) in enumerate(tasks, start=1):
        tokens = args.tokens_per_scale * scale
        print(
            f"memory {index}/{len(tasks)}: {variant} {scale}x rep {repetition + 1}",
            file=sys.stderr,
            flush=True,
        )
        worker_result = _run_worker(
            [
                "--worker",
                "memory",
                "--variant",
                variant,
                "--tokens",
                str(tokens),
                "--num-permutations",
                str(args.num_permutations),
            ]
        )
        if worker_result["ordinal"] != 17:
            raise AssertionError("sibling field changed")
        memory_samples[variant][str(scale)].append(worker_result["max_rss_bytes"])
        signatures[variant][str(scale)].add(worker_result["signature"])

    for scale in args.scales:
        scale_key = str(scale)
        if len(signatures["baseline"][scale_key]) != 1:
            raise AssertionError(f"baseline signature was unstable at scale {scale}")
        if signatures["baseline"][scale_key] != signatures["blockwise"][scale_key]:
            raise AssertionError(f"A/B signatures differed at scale {scale}")

    latency_samples = {variant: [] for variant in _VARIANTS}
    latency_signatures = {variant: set() for variant in _VARIANTS}
    latency_tasks = [(variant, repetition) for variant in _VARIANTS for repetition in range(args.latency_repetitions)]
    random.Random(args.seed + 1).shuffle(latency_tasks)
    for index, (variant, repetition) in enumerate(latency_tasks, start=1):
        print(
            f"latency {index}/{len(latency_tasks)}: {variant} rep {repetition + 1}",
            file=sys.stderr,
            flush=True,
        )
        worker_result = _run_worker(
            [
                "--worker",
                "latency",
                "--variant",
                variant,
                "--tokens",
                str(args.latency_tokens),
                "--num-permutations",
                str(args.num_permutations),
                "--warmup-calls",
                str(args.warmup_calls),
                "--latency-observations",
                str(args.latency_observations),
            ]
        )
        latency_samples[variant].extend(worker_result["seconds_per_call"])
        latency_signatures[variant].add(worker_result["signature"])
    if latency_signatures["baseline"] != latency_signatures["blockwise"]:
        raise AssertionError("latency A/B signatures differed")

    result = {
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "parameters": {
            "tokens_per_scale": args.tokens_per_scale,
            "scales": args.scales,
            "memory_repetitions": args.memory_repetitions,
            "num_permutations": args.num_permutations,
            "latency_tokens": args.latency_tokens,
            "latency_repetitions": args.latency_repetitions,
            "latency_observations": args.latency_observations,
            "warmup_calls": args.warmup_calls,
            "seed": args.seed,
        },
        "memory": {
            "samples": memory_samples,
            "signatures": {
                variant: {scale: sorted(values) for scale, values in by_scale.items()}
                for variant, by_scale in signatures.items()
            },
            "summary": _summarize_memory(memory_samples, args.tokens_per_scale, args.scales),
        },
        "latency": {
            "samples": latency_samples,
            "summary": _summarize_latency(latency_samples),
        },
    }
    _print_markdown(result)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tokens-per-scale", type=int, default=32 * 1024)
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--memory-repetitions", type=int, default=5)
    parser.add_argument("--num-permutations", type=int, default=256)
    parser.add_argument("--latency-tokens", type=int, default=8 * 1024)
    parser.add_argument("--latency-repetitions", type=int, default=3)
    parser.add_argument("--latency-observations", type=int, default=40)
    parser.add_argument("--warmup-calls", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260805)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--worker", choices=("memory", "latency"), help=argparse.SUPPRESS)
    parser.add_argument("--variant", choices=_VARIANTS, help=argparse.SUPPRESS)
    parser.add_argument("--tokens", type=int, help=argparse.SUPPRESS)
    return parser.parse_args()


def main():
    args = _parse_args()
    if args.worker:
        if args.variant is None or args.tokens is None:
            raise ValueError("worker mode requires variant and tokens")
        if args.tokens <= 0 or args.num_permutations <= 0 or args.num_permutations % 32:
            raise ValueError("tokens must be positive and num_permutations must be a positive multiple of 32")
        if args.worker == "memory":
            _memory_worker(args)
        else:
            _latency_worker(args)
        return

    positive_values = (
        args.tokens_per_scale,
        args.memory_repetitions,
        args.num_permutations,
        args.latency_tokens,
        args.latency_repetitions,
        args.latency_observations,
        args.warmup_calls,
    )
    if any(value <= 0 for value in positive_values):
        raise ValueError("benchmark sizes and repetition counts must be positive")
    if args.num_permutations % 32:
        raise ValueError("num_permutations must be divisible by 32")
    if len(args.scales) < 2 or len(set(args.scales)) != len(args.scales) or any(scale <= 0 for scale in args.scales):
        raise ValueError("at least two unique positive scales are required")
    args.scales.sort()
    _benchmark(args)


if __name__ == "__main__":
    main()
