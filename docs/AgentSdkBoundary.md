# Agent SDK Boundary Guide

Use this guide when a failure crosses from Data-Juicer into Ray/byted-ray, PyArrow, PyIceberg, Magnus, Lance, datasinks, runtime-env packaging, or another third-party/internal SDK.

## Boundary Principles

- Verify the real SDK or deployed package contract before patching Data-Juicer integration code.
- Do not rely only on mocks of the SDK entry function.
- When a stack trace enters a third-party SDK, read the next layer of that SDK code before patching.
- Fix both sides of write/export integrations: call arguments must be correct, and completion/commit callbacks must accept the returned structure.
- For third-party boundary code, mock tests are not sufficient when they mock away the real contract. Add a contract test that models the third-party input shape, such as Ray `WriteResult(write_returns=...)` for `Datasink.on_write_complete`.

## Local Source Checkouts

- For Ray/byted-ray/DataFusion/PyArrow boundary failures, do not rely on open-source Ray API behavior as the source of truth. The bytedray/Ray source checkout lives at `../ray`; inspect it before deciding whether Data-Juicer should filter arguments, change config syntax, or adapt to an SDK contract.
- For PyIceberg/Magnus/Lance boundary failures, inspect the local PyIceberg source checkout at `../iceberg` before patching Data-Juicer integration code or assuming SDK behavior.
- If the local environment lacks the target SDK, inspect the wheel/source for the deployed version or the closest compatible wheel. State which version was inspected.

## Contract Checks

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

## Runtime Env And Working Directory

- When a failure involves Ray `working_dir`, remember that packaging and `gcs://_ray_pkg_*.zip` distribution are Ray runtime-env behavior.
- Use `.rayignore` or submitter-side `runtime_env.excludes` for Ray packaging. `.dockerignore` does not affect Ray packaging.
- For dependency/runtime-env changes, keep pure logic tests separate from network or package-install tests. Pure tests should not require downloading `opencc`, `torch`, model weights, or internal wheels.
