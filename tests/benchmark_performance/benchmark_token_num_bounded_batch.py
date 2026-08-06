#!/usr/bin/env python3
"""Fresh-process A/B benchmark for bounded token-count batches."""

import argparse
import gc
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
import tracemalloc
from pathlib import Path
from unittest import mock

import numpy as np

from data_juicer.ops.filter import token_num_filter as token_num_module
from data_juicer.ops.filter.token_num_filter import TokenNumFilter
from data_juicer.utils.constant import Fields, StatsKeys

_SCRIPT_PATH = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT_PATH.parents[2]
_RESULT_PREFIX = "TOKEN_NUM_BENCHMARK_RESULT="
_VARIANTS = ("baseline", "bounded")
_TOKENIZER_KINDS = ("controlled", "default-hf")


class _EagerTokenNumFilter(TokenNumFilter):
    def compute_stats_batched(self, samples, *args, **kwargs):
        samples_list = samples[self.text_key]
        samples_stats = samples[Fields.stats]

        indices = []
        texts = []
        for idx, stat in enumerate(samples_stats):
            if StatsKeys.num_token not in stat:
                indices.append(idx)
                texts.append(samples_list[idx])

        if texts:
            tokenizer = token_num_module.get_model(self.model_key)
            encoded = tokenizer(texts, add_special_tokens=False)
            for index, idx in enumerate(indices):
                samples_stats[idx][StatsKeys.num_token] = len(encoded["input_ids"][index])

        return samples


class _ControlledTokenizer:
    def __init__(self, tokens_per_row):
        self.tokens_per_row = tokens_per_row
        self.batch_sizes = []

    def __call__(self, texts, **_kwargs):
        self.batch_sizes.append(len(texts))
        input_ids = [
            [(row_index * 997 + token_index) % 50_000 for token_index in range(self.tokens_per_row)]
            for row_index, _text in enumerate(texts)
        ]
        return {
            "input_ids": input_ids,
            "attention_mask": [[1] * self.tokens_per_row for _text in texts],
        }


class _RecordingTokenizer:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.batch_sizes = []

    def __call__(self, texts, **kwargs):
        self.batch_sizes.append(len(texts))
        return self.tokenizer(texts, **kwargs)


def _new_op(variant):
    cls = _EagerTokenNumFilter if variant == "baseline" else TokenNumFilter
    return cls(hf_tokenizer="EleutherAI/pythia-6.9b-deduped", auto_op_parallelism=False, num_proc=1)


def _texts(row_count, tokens_per_row, tokenizer_kind):
    if tokenizer_kind == "controlled":
        return [f"row-{index}" for index in range(row_count)]
    body = " data" * tokens_per_row
    return [f"row-{index}{body}" for index in range(row_count)]


def _tokenizer(op, tokenizer_kind, tokens_per_row):
    if tokenizer_kind == "controlled":
        return _ControlledTokenizer(tokens_per_row)
    return _RecordingTokenizer(token_num_module.get_model(op.model_key))


def _counts_digest(samples):
    counts = [stat[StatsKeys.num_token] for stat in samples[Fields.stats]]
    digest = hashlib.sha256()
    for count in counts:
        digest.update(count.to_bytes(8, "big"))
    return {
        "sha256": digest.hexdigest(),
        "minimum": min(counts),
        "maximum": max(counts),
    }


def _run_compute(op, tokenizer, texts):
    marker = object()
    samples = {
        "text": texts,
        Fields.stats: [{} for _text in texts],
        "marker": marker,
    }
    with mock.patch.object(token_num_module, "get_model", return_value=tokenizer):
        returned = op.compute_stats_batched(samples)
    if returned is not samples or returned["marker"] is not marker:
        raise AssertionError("operator changed the batch object or a sibling field")
    return samples


def _max_rss_bytes():
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(max_rss if sys.platform == "darwin" else max_rss * 1024)


def _memory_worker(args):
    op = _new_op(args.variant)
    tokenizer = _tokenizer(op, args.tokenizer_kind, args.tokens_per_row)
    texts = _texts(args.rows, args.tokens_per_row, args.tokenizer_kind)
    gc.collect()
    samples = _run_compute(op, tokenizer, texts)
    print(
        _RESULT_PREFIX
        + json.dumps(
            {
                "max_rss_bytes": _max_rss_bytes(),
                "counts": _counts_digest(samples),
                "tokenizer_batch_sizes": tokenizer.batch_sizes,
            }
        )
    )


def _allocation_worker(args):
    op = _new_op(args.variant)
    tokenizer = _tokenizer(op, args.tokenizer_kind, args.tokens_per_row)
    texts = _texts(args.rows, args.tokens_per_row, args.tokenizer_kind)
    gc.collect()
    tracemalloc.start()
    samples = _run_compute(op, tokenizer, texts)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(
        _RESULT_PREFIX
        + json.dumps(
            {
                "tracemalloc_peak_bytes": peak,
                "counts": _counts_digest(samples),
                "tokenizer_batch_sizes": tokenizer.batch_sizes,
            }
        )
    )


def _latency_worker(args):
    op = _new_op(args.variant)
    tokenizer = _tokenizer(op, args.tokenizer_kind, args.tokens_per_row)
    texts = _texts(args.rows, args.tokens_per_row, args.tokenizer_kind)

    for _index in range(args.warmup_calls):
        _run_compute(op, tokenizer, texts)
    tokenizer.batch_sizes.clear()

    observations = []
    signature = None
    for _index in range(args.latency_observations):
        started = time.perf_counter()
        samples = _run_compute(op, tokenizer, texts)
        observations.append(time.perf_counter() - started)
        current_signature = _counts_digest(samples)
        if signature is None:
            signature = current_signature
        elif signature != current_signature:
            raise AssertionError("token counts changed between latency observations")

    print(
        _RESULT_PREFIX
        + json.dumps(
            {
                "seconds_per_call": observations,
                "counts": signature,
                "tokenizer_batch_sizes": tokenizer.batch_sizes,
            }
        )
    )


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


def _summarize_memory(samples, memory_rows, metric):
    by_rows = {}
    medians = {variant: [] for variant in _VARIANTS}
    for rows in memory_rows:
        key = str(rows)
        baseline = statistics.median(samples["baseline"][key])
        bounded = statistics.median(samples["bounded"][key])
        medians["baseline"].append(baseline)
        medians["bounded"].append(bounded)
        by_rows[key] = {
            "baseline_median_bytes": baseline,
            "bounded_median_bytes": bounded,
            "reduction": 1 - bounded / baseline,
        }
    baseline_slope = _linear_slope(memory_rows, medians["baseline"])
    bounded_slope = _linear_slope(memory_rows, medians["bounded"])
    slope_reduction = None if baseline_slope <= 0 or bounded_slope < 0 else 1 - bounded_slope / baseline_slope
    return {
        "metric": metric,
        "by_rows": by_rows,
        "growth": {
            "baseline_bytes_per_row": baseline_slope,
            "bounded_bytes_per_row": bounded_slope,
            "slope_reduction": slope_reduction,
        },
    }


def _summarize_latency(samples):
    baseline = samples["baseline"]
    bounded = samples["bounded"]
    baseline_median = statistics.median(baseline)
    bounded_median = statistics.median(bounded)
    return {
        "baseline_median_seconds": baseline_median,
        "bounded_median_seconds": bounded_median,
        "throughput_ratio": baseline_median / bounded_median,
        "baseline_p95_seconds": _percentile(baseline, 0.95),
        "bounded_p95_seconds": _percentile(bounded, 0.95),
        "p95_latency_ratio": _percentile(bounded, 0.95) / _percentile(baseline, 0.95),
    }


def _assert_worker_result(result, variant, rows, calls=1):
    batch_sizes = result["tokenizer_batch_sizes"]
    expected_rows = rows * calls
    if sum(batch_sizes) != expected_rows:
        raise AssertionError(f"{variant} tokenized {sum(batch_sizes)} rows; expected {expected_rows}")
    if variant == "baseline" and batch_sizes != [rows] * calls:
        raise AssertionError(f"eager baseline used unexpected batches: {batch_sizes}")
    if variant == "bounded" and max(batch_sizes) > 128:
        raise AssertionError(f"bounded implementation exceeded 128 rows: {batch_sizes}")


def _collect_memory(args, worker, metric):
    samples = {variant: {str(rows): [] for rows in args.memory_rows} for variant in _VARIANTS}
    signatures = {variant: {str(rows): set() for rows in args.memory_rows} for variant in _VARIANTS}
    tasks = [
        (variant, rows, repetition)
        for variant in _VARIANTS
        for rows in args.memory_rows
        for repetition in range(args.memory_repetitions)
    ]
    random.Random(args.seed + (0 if worker == "memory" else 1)).shuffle(tasks)
    for index, (variant, rows, repetition) in enumerate(tasks, start=1):
        print(
            f"{worker} {index}/{len(tasks)}: {variant} rows={rows} rep={repetition + 1}",
            file=sys.stderr,
            flush=True,
        )
        result = _run_worker(
            [
                "--worker",
                worker,
                "--variant",
                variant,
                "--rows",
                str(rows),
                "--tokens-per-row",
                str(args.tokens_per_memory_row),
                "--tokenizer-kind",
                args.tokenizer_kind,
            ]
        )
        _assert_worker_result(result, variant, rows)
        samples[variant][str(rows)].append(result[metric])
        signatures[variant][str(rows)].add(json.dumps(result["counts"], sort_keys=True))

    for rows in args.memory_rows:
        key = str(rows)
        if len(signatures["baseline"][key]) != 1:
            raise AssertionError(f"baseline counts were unstable at {rows} rows")
        if signatures["baseline"][key] != signatures["bounded"][key]:
            raise AssertionError(f"A/B token counts differed at {rows} rows")
    return {
        "samples": samples,
        "signatures": {
            variant: {rows: sorted(values) for rows, values in by_rows.items()}
            for variant, by_rows in signatures.items()
        },
        "summary": _summarize_memory(samples, args.memory_rows, metric),
    }


def _collect_latency(args):
    samples = {variant: [] for variant in _VARIANTS}
    signatures = {variant: set() for variant in _VARIANTS}
    tasks = [(variant, repetition) for variant in _VARIANTS for repetition in range(args.latency_repetitions)]
    random.Random(args.seed + 2).shuffle(tasks)
    for index, (variant, repetition) in enumerate(tasks, start=1):
        print(
            f"latency {index}/{len(tasks)}: {variant} rep={repetition + 1}",
            file=sys.stderr,
            flush=True,
        )
        result = _run_worker(
            [
                "--worker",
                "latency",
                "--variant",
                variant,
                "--rows",
                str(args.latency_rows),
                "--tokens-per-row",
                str(args.tokens_per_latency_row),
                "--tokenizer-kind",
                args.tokenizer_kind,
                "--latency-observations",
                str(args.latency_observations),
                "--warmup-calls",
                str(args.warmup_calls),
            ]
        )
        _assert_worker_result(
            {"tokenizer_batch_sizes": result["tokenizer_batch_sizes"]},
            variant,
            args.latency_rows,
            calls=args.latency_observations,
        )
        samples[variant].extend(result["seconds_per_call"])
        signatures[variant].add(json.dumps(result["counts"], sort_keys=True))
    if signatures["baseline"] != signatures["bounded"]:
        raise AssertionError("A/B token counts differed in latency workload")
    return {
        "samples": samples,
        "signatures": {variant: sorted(values) for variant, values in signatures.items()},
        "summary": _summarize_latency(samples),
    }


def _print_markdown(result):
    if "rss" in result:
        print("## Fresh-process peak RSS")
        print()
        print("| Rows | Eager MiB | Bounded MiB | Reduction |")
        print("| ---: | ---: | ---: | ---: |")
        for rows, values in result["rss"]["summary"]["by_rows"].items():
            print(
                f"| {int(rows):,} | {values['baseline_median_bytes'] / 1024**2:.1f} | "
                f"{values['bounded_median_bytes'] / 1024**2:.1f} | {values['reduction']:.1%} |"
            )

    if "allocation" in result:
        print()
        print("## Algorithm-local Python allocation peak")
        print()
        print("| Rows | Eager MiB | Bounded MiB | Reduction |")
        print("| ---: | ---: | ---: | ---: |")
        for rows, values in result["allocation"]["summary"]["by_rows"].items():
            print(
                f"| {int(rows):,} | {values['baseline_median_bytes'] / 1024**2:.1f} | "
                f"{values['bounded_median_bytes'] / 1024**2:.1f} | {values['reduction']:.1%} |"
            )

    latency = result["latency"]["summary"]
    print()
    print("## Latency")
    print()
    print("| Eager median ms | Bounded median ms | Throughput ratio | Bounded/eager P95 |")
    print("| ---: | ---: | ---: | ---: |")
    print(
        f"| {latency['baseline_median_seconds'] * 1000:.3f} | "
        f"{latency['bounded_median_seconds'] * 1000:.3f} | "
        f"{latency['throughput_ratio']:.3f}x | {latency['p95_latency_ratio']:.3f}x |"
    )


def _benchmark(args):
    result = {
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "parameters": {
            "tokenizer_kind": args.tokenizer_kind,
            "memory_rows": args.memory_rows,
            "tokens_per_memory_row": args.tokens_per_memory_row,
            "memory_repetitions": args.memory_repetitions,
            "latency_rows": args.latency_rows,
            "tokens_per_latency_row": args.tokens_per_latency_row,
            "latency_repetitions": args.latency_repetitions,
            "latency_observations": args.latency_observations,
            "warmup_calls": args.warmup_calls,
            "seed": args.seed,
        },
    }
    if not args.latency_only:
        result["rss"] = _collect_memory(args, "memory", "max_rss_bytes")
        result["allocation"] = _collect_memory(args, "allocation", "tracemalloc_peak_bytes")
    result["latency"] = _collect_latency(args)
    _print_markdown(result)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tokenizer-kind", choices=_TOKENIZER_KINDS, default="controlled")
    parser.add_argument("--memory-rows", type=int, nargs="+", default=[128, 512, 1000])
    parser.add_argument("--tokens-per-memory-row", type=int, default=1024)
    parser.add_argument("--memory-repetitions", type=int, default=5)
    parser.add_argument("--latency-rows", type=int, default=256)
    parser.add_argument("--tokens-per-latency-row", type=int, default=1024)
    parser.add_argument("--latency-repetitions", type=int, default=3)
    parser.add_argument("--latency-observations", type=int, default=20)
    parser.add_argument("--warmup-calls", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260806)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--latency-only", action="store_true")
    parser.add_argument("--worker", choices=("memory", "allocation", "latency"), help=argparse.SUPPRESS)
    parser.add_argument("--variant", choices=_VARIANTS, help=argparse.SUPPRESS)
    parser.add_argument("--rows", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--tokens-per-row", type=int, help=argparse.SUPPRESS)
    return parser.parse_args()


def main():
    args = _parse_args()
    if args.worker:
        if args.variant is None or args.rows is None or args.tokens_per_row is None:
            raise ValueError("worker mode requires variant, rows, and tokens-per-row")
        if args.rows <= 0 or args.tokens_per_row <= 0:
            raise ValueError("worker sizes must be positive")
        if args.worker == "memory":
            _memory_worker(args)
        elif args.worker == "allocation":
            _allocation_worker(args)
        else:
            _latency_worker(args)
        return

    positive_values = (
        args.tokens_per_memory_row,
        args.memory_repetitions,
        args.latency_rows,
        args.tokens_per_latency_row,
        args.latency_repetitions,
        args.latency_observations,
        args.warmup_calls,
    )
    if any(value <= 0 for value in positive_values):
        raise ValueError("benchmark sizes and repetition counts must be positive")
    if len(args.memory_rows) < 2 or len(set(args.memory_rows)) != len(args.memory_rows):
        raise ValueError("at least two unique memory row counts are required")
    if any(rows <= 0 for rows in args.memory_rows):
        raise ValueError("memory row counts must be positive")
    args.memory_rows.sort()
    _benchmark(args)


if __name__ == "__main__":
    main()
