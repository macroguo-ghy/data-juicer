# Agent Testing Guide

Use this guide when a change needs concrete verification commands, coverage scope, or a focused fallback runner. Keep `AGENTS.md` short; put command-heavy test details here.

## TDD Gate

Before editing production code for a clear bug fix or behavior change:

1. Name the observable behavior that should change.
2. Add or update the smallest focused test for that behavior.
3. Run that test and record whether it fails for the expected reason.
4. Make the minimal production change.
5. Rerun the focused test, then broaden only if risk warrants it.

Allowed exceptions:

- docs-only changes
- pure investigation with no code change
- test environment blocked by missing dependency or internal service
- emergency mechanical changes where no meaningful behavior test exists

When an exception applies, say it explicitly in the final handoff. Do not silently replace TDD with only `py_compile`, import checks, or mocks that skip the contract under test.

## Behavior Coverage

- Every behavior change must add or update unit tests.
- New code should meet at least 90% test coverage for the changed behavior. Treat coverage as a minimum quality gate, not the goal of the tests.
- Do not write tests only to execute lines. Test realistic production failure modes and corner cases that could break a task.
- Add the smallest unit test that reproduces the bug before or with the fix.
- Test the real control path, not only a nearby setup path.

Examples:

- If a bug depends on an empty `OPEnvSpec` being combined with a dependency-bearing spec, test that exact combination directly.
- For Data-Juicer configs, `init_configs(..., load_configs_only=True)` is not equivalent to `load_ops(cfg.process, op_env_manager)` or `executor.run()`.
- When changing `OPEnvManager`, `OPEnvSpec`, lazy dependency analysis, or operator loading, include tests under `tests/ops/test_op_env.py` or the nearest existing test file.
- When changing IO behavior, add tests for both the config-facing path and the concrete read/write helper where practical.
- When changing demos, add a smoke test or a minimal scriptable verification that covers the key path the demo is meant to exercise. For Ray demos, also run `--ray_dry_run_plan True` when practical and check that the logical and physical plans match the expected pipeline before running or submitting the full job.

## Contract-First Testing

For clear bug fixes and behavior changes, use contract-first TDD:

- Tests define observable behavior through the public interface, not private methods, internal call order, or imagined implementation shape.
- Use vertical red-green-refactor slices. Write one failing test for one behavior, implement the minimal production change to make it pass, then repeat with the next learned behavior.
- Refactor only after the relevant tests are green. During refactor, keep tests pointed at the same public contract and rerun the focused suite after each meaningful change.
- For risky changes, test both sides of the contract: the case that must now pass and the opposite case that must still fail.
- Treat rerun and idempotent paths as first-class cases for exporters, table creation, partition writes, directory creation, task submission, and registration flows.
- Put validation in the layer that owns the needed context. Callers should not reject before the helper or SDK boundary checks external state such as whether a table, path, partition, or task exists.
- Mock external systems, not the Data-Juicer helper whose contract is under test. For example, mock `MagnusClient.exist_table/load_table/create_table`, but exercise `create_magnus_table_if_not_exists` unless a separate test covers it directly.

Check this matrix before choosing test scope for config switches, IO adapters, exporters, SDK boundaries, and distributed runtime paths:

```text
resource state: missing / already exists
config state: complete / missing / conflicting
execution mode: first run / rerun / append / overwrite
validation point: caller / project helper / SDK boundary
```

## Ray Data And PyArrow Tests

For Ray Data and PyArrow paths, test block-level schema stability, not just Python dict or single-row behavior.

When changing Ray execution shape, such as replacing a `map_batches` or
`compute_stats` path with `Dataset.filter` or another direct row callback, test
the value representation used by the new Ray callback. Do not only test Python
dict rows or fake datasets.

For row-wise Ray callbacks, include nullable Arrow scalar cases such as
`pa.scalar(None, type=pa.int64())` and non-null Arrow scalar values when the
logic compares, filters, or serializes fields. Verify that the new path
preserves null and missing-field semantics from the old path, especially when
the old path used `to_pydict()`, Arrow batch conversion, stats/meta columns, or
schema normalization.

Include production-shaped corner cases when an operator adds or rewrites columns:

- all-null blocks
- mixed null/non-null blocks
- empty lists
- bytes/list fields
- nested dicts
- empty batches
- multiple Ray blocks being concatenated

## Focused Commands

Use focused commands first, then broaden if the risk is higher.

Syntax checks are useful but insufficient:

```bash
python3 -m py_compile <changed-python-files>
```

For coverage checks on changed behavior, prefer focused pytest coverage runs before broadening:

```bash
./.venv/bin/python -m pytest <relevant-test-files-or-test-names> \
  --cov=<changed-package-or-module> \
  --cov-report=term-missing \
  --cov-fail-under=90
```

For OP environment changes, run the relevant unit tests:

```bash
./.venv/bin/python -m unittest tests.ops.test_op_env.OPEnvSpecTest tests.ops.test_op_env.OPEnvManagerTest
```

If the standard test base triggers unrelated heavyweight lazy dependencies such as `torch`, run a narrowly patched local runner and state that explicitly in the handoff:

```bash
./.venv/bin/python - <<'PY'
import unittest
import data_juicer.utils.unittest_utils as uu

uu.free_models = lambda *args, **kwargs: None

from tests.ops import test_op_env

suite = unittest.TestSuite()
loader = unittest.TestLoader()
suite.addTests(loader.loadTestsFromTestCase(test_op_env.OPEnvSpecTest))
suite.addTests(loader.loadTestsFromTestCase(test_op_env.OPEnvManagerTest))
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
PY
```

For the lazy dependency demo, the minimum non-Ray behavior check should cover the dependency merge path:

```bash
./.venv/bin/python - <<'PY'
from data_juicer.ops.op_env import OPEnvManager, OPEnvSpec

manager = OPEnvManager(min_common_dep_num_to_combine=0)
manager.record_op_env_spec("empty_op", OPEnvSpec())
manager.record_op_env_spec("dependency_op", OPEnvSpec(pip_pkgs=["opencc"]))

assert manager.get_op_env_spec("empty_op").pip_pkgs == ["opencc"]
assert manager.get_op_env_spec("dependency_op").pip_pkgs == ["opencc"]
PY
```

If a full integration run is blocked by missing internal services, unavailable Magnus credentials, Ray cluster limits, network restrictions, or heavyweight dependency installation, say exactly what was blocked and what narrower verification did run.
