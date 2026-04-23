# Repository Guidelines

## Project Baseline

- Work from the repository root: `/Users/bytedance/repo/data-juicer`.
- Main package code lives under `data_juicer/`; CLI entry points and utility scripts live under `tools/`; tests live under `tests/`; runnable examples live under `demos/`.
- Keep changes scoped to the requested behavior. Do not refactor unrelated operators, configs, Docker files, or demos while fixing a narrow issue.
- Preserve local-only files. Do not commit generated files such as `yaml.base64`, `outputs/`, caches, virtual environments, or temporary artifacts.

## Testing Requirements

- Every behavior change must add or update unit tests. This is mandatory for bug fixes, new demos, config parsing behavior, runtime-env behavior, dependency resolution, IO adapters, and operator loading.
- A syntax check such as `python -m py_compile ...` is not enough. It only proves the files compile; it does not prove object state, branch behavior, operator loading, or Ray/Data-Juicer runtime paths work.
- Add the smallest unit test that reproduces the bug before or with the fix. For example, if a bug depends on an empty `OPEnvSpec` being combined with a dependency-bearing spec, test that exact combination directly.
- Test the real control path, not only a nearby setup path. For Data-Juicer configs, `init_configs(..., load_configs_only=True)` is not equivalent to `load_ops(cfg.process, op_env_manager)` or `executor.run()`.
- When changing `OPEnvManager`, `OPEnvSpec`, lazy dependency analysis, or operator loading, include tests under `tests/ops/test_op_env.py` or the nearest existing test file.
- When changing IO behavior, add tests for both the config-facing path and the concrete read/write helper where practical.
- When changing calls into third-party or internal SDKs, verify the real API contract. Do not rely only on mocks of the SDK entry function.
- When changing demos, add a smoke test or a minimal scriptable verification that covers the key path the demo is meant to exercise.

## Verification Commands

Use focused commands first, then broaden if the risk is higher:

```bash
python3 -m py_compile <changed-python-files>
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

For Magnus/Ray SDK boundary changes, verify the installed or target package contract before patching:

```bash
./.venv/bin/python - <<'PY'
import inspect
import pyiceberg.ray

print(inspect.signature(pyiceberg.ray.write_magnus))
PY
```

If the package is not installed locally, download the target wheel and inspect the source instead of guessing the signature:

```bash
python3 -m pip download --no-deps \
  --dest /tmp/byted_iceberg_inspect \
  -i https://bytedpypi.byted.org/simple \
  --trusted-host bytedpypi.byted.org \
  byted-iceberg==<target-version>
```

For Ray datasink changes, inspect the current Ray callback contract:

```bash
./.venv/bin/python - <<'PY'
import inspect
from ray.data.dataset import Dataset

print(inspect.getsource(Dataset.write_datasink))
PY
```

## Avoiding Shallow Fixes

- After fixing one failure, continue to the next meaningful stage of the workflow before stopping. For Ray Data-Juicer jobs, think in stages:
  `config parse -> operator load -> OPEnvManager runtime_env generation -> Ray working_dir distribution -> Ray task startup -> operator execution -> export`.
- Do not declare a Ray/Data-Juicer issue fixed only because the previous stack trace disappeared. Verify the next stage starts successfully or document the new blocker.
- For third-party boundary code, mock tests are not sufficient when they mock away the real contract. Add a contract test that models the third-party input shape, such as Ray `WriteResult(write_returns=...)` for `Datasink.on_write_complete`.
- If a local environment lacks the target SDK, inspect the wheel/source for the deployed version or the closest compatible wheel. State which version was inspected.
- When a stack trace enters a third-party SDK, read the next layer of that SDK code before patching. Fixing only the immediate caller often misses required parameters or callback data-shape changes.
- For write/export integrations, verify both "call arguments are correct" and "completion/commit callback accepts the returned structure".
- For dynamic Python attributes, make constructors establish object invariants on every branch. Empty/default objects must still have all attributes that public methods access.
- For dependency/runtime-env changes, separate pure logic tests from network or package-install tests. Pure tests should not require downloading `opencc`, `torch`, model weights, or internal wheels.
- When a failure involves Ray `working_dir`, remember that packaging and `gcs://_ray_pkg_*.zip` distribution are Ray runtime-env behavior. Use `.rayignore` or submitter-side `runtime_env.excludes`; `.dockerignore` does not affect Ray packaging.
- When using OPEnvManager with `min_common_dep_num_to_combine: 0`, explicitly test empty dependency specs combined with non-empty specs.

## Commit And Handoff

- Before committing, run `git status --short` and review the staged diff.
- Do not include unrelated local files in commits.
- In the final handoff, list the exact tests or smoke checks run. If any expected test could not run, include the reason and the substitute verification.
