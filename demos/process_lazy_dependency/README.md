# Lazy Dependency Demo

This demo verifies that Data-Juicer can install an operator dependency through
`OPEnvManager` and Ray runtime env.

It runs with `executor_type: ray`, reads from Magnus, and writes back to Magnus.
The process includes `chinese_convert_mapper`, which lazy-loads the `opencc` package.
The config enables `OPEnvManager` with `min_common_dep_num_to_combine: 0`, so
Data-Juicer analyzes the operator's `LazyLoader("opencc")`, builds an operator
runtime env, and passes it to Ray before executing the operator.

Source table:

- `ghy_test.default.lance_format`
- schema: `id long; name string; score double`

Target table:

- `ghy_test.default.lance_format_output3`
- schema: `id long; name string; score double; processed_by string`
- write operation: `OVERWRITE`

Config:

- `demos/process_lazy_dependency/configs/opencc_demo.yaml`

Run the process job:

```bash
dj-process --config demos/process_lazy_dependency/configs/opencc_demo.yaml
```

The demo connects to the current Ray cluster by default:

```yaml
executor_type: "ray"
ray_address: "auto"
min_common_dep_num_to_combine: 0
```

For a local-only check, override the address:

```bash
dj-process \
  --config demos/process_lazy_dependency/configs/opencc_demo.yaml \
  --ray_address local
```

When this is submitted through Ray Jobs, the job-level `working_dir` package is
managed by the submitter before Data-Juicer starts. If workers fail to download a
large `gcs://_ray_pkg_*.zip`, configure the submitter's runtime env to exclude
`.git`, caches, build outputs, and local artifacts, or increase
`RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S` for long uploads.

Expected output:

```text
ghy_test.default.lance_format_output3
```

Rows with `score >= 85.0` should be written with `processed_by` first set to
`数据处理依赖测试`, then converted by `chinese_convert_mapper` to Traditional Chinese.
The source table does not need Chinese text because the demo creates the Chinese
`processed_by` value before running the dependency-triggering operator. The table schema
stays `id long; name string; score double; processed_by string`.
