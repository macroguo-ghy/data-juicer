# Dataset Export

This document describes how DataJuicer exports processed datasets, including supported formats, sharding, parallel export, S3 export, and stats/hash management.

## Overview

After processing, DataJuicer exports the result dataset to disk using the `Exporter` (default mode) or `RayExporter` (Ray mode). The export system supports:

- **Multiple output formats** — JSONL, JSON, Parquet, and more in Ray mode
- **Writer row/file controls** — pass supported sink-specific writer options through `export.extra_args`
- **Parallel export** — speed up single-file export with multiprocessing
- **S3 export** — write results directly to Amazon S3 or S3-compatible storage
- **Row limit** — cap the number of rows passed to the export sink with structured export config
- **Stats and hash management** — control which intermediate fields are kept in the output

## Configuration

### Basic Settings

```yaml
export_path: ./outputs/result.jsonl       # Output file path (required)
export_type: jsonl                         # Format type (auto-detected from path if omitted)
export_shard_size: 0                       # Deprecated and disabled; keep 0 as a compatibility placeholder
export_in_parallel: false                  # Parallel export for single-file mode
keep_stats_in_res_ds: false                # Keep computed stats in output
keep_hashes_in_res_ds: false               # Keep computed hashes in output
export_extra_args: {}                      # Additional format-specific arguments
export_aws_credentials: null               # For S3 export, see S3 section for details
```

For row-limited export, use the structured `export` config:

```yaml
export:
  target: local
  path: ./outputs/result.jsonl
  type: jsonl
  max_rows: 1000
  max_rows_mode: limit
```

### Command Line

```bash
# Basic export
dj-process --config config.yaml --export_path ./outputs/result.jsonl

# Export as Parquet
dj-process --config config.yaml --export_path ./outputs/result.parquet

# Control writer file sizes/row groups with supported sink-specific extra_args
# (for example, min_rows_per_file or num_rows_per_file in Ray writers)

# Keep stats in output
dj-process --config config.yaml --keep_stats_in_res_ds true
```

## Supported Formats

### Default Mode (Exporter)

| Format | Suffix | Description |
|--------|--------|-------------|
| JSONL | `.jsonl` | JSON Lines — one JSON object per line (default) |
| JSON | `.json` | Standard JSON array |
| Parquet | `.parquet` | Columnar format, efficient for large datasets |

### Ray Mode (RayExporter)

| Format | Suffix | Description |
|--------|--------|-------------|
| JSONL | `.jsonl` | JSON Lines |
| JSON | `.json` | Standard JSON |
| Parquet | `.parquet` | Columnar format |
| CSV | `.csv` | Comma-separated values |
| TFRecords | `.tfrecords` | TensorFlow record format |
| WebDataset | `webdataset` | WebDataset tar-based format |
| Lance | `.lance` | Lance columnar format |

## Shard Export

`export_shard_size`, `export.shard_size`, and `export.export_shard_size` are deprecated and disabled. Keep the top-level `export_shard_size` value at `0` only as a compatibility placeholder.

For Ray HDFS and Ray file writers, control output file counts with supported writer options in `export.extra_args`, for example:

```yaml
export:
  target: hdfs
  path: hdfs://cluster/path/output_dir
  type: parquet
  extra_args:
    min_rows_per_file: 200000
    concurrency: 128
```

## Parallel Export

For single-file export, enable parallel writing to speed up the process:

```yaml
export_path: ./outputs/result.jsonl
export_in_parallel: true
np: 4                                     # Number of parallel processes
```

**Important**: Parallel export can sometimes be **slower** than sequential export due to IO blocking, especially for very large datasets. If you observe this, set `export_in_parallel: false`.

`export_shard_size > 0` is no longer supported.

## S3 Export

Export results directly to Amazon S3 or S3-compatible storage.

### Default Mode

```yaml
export_path: "s3://my-bucket/outputs/result.jsonl"
export_aws_credentials:
  aws_access_key_id: "AKIA..."
  aws_secret_access_key: "secret..."
  aws_region: "us-east-1"
  endpoint_url: "https://s3.example.com"   # Optional: for S3-compatible storage
```

The default exporter uses HuggingFace's `storage_options` with `fsspec`/`s3fs` for S3 access.

### Ray Mode

```yaml
export_path: "s3://my-bucket/outputs/result.jsonl"
export_extra_args:
  aws_access_key_id: "AKIA..."
  aws_secret_access_key: "secret..."
  aws_region: "us-east-1"
```

The Ray exporter uses PyArrow's S3 filesystem for S3 access.

### S3 with Sharding

`export_shard_size` based sharding is deprecated and disabled. Use supported writer arguments in `export_extra_args` or `export.extra_args` where the selected writer supports them.

### Credential Resolution

AWS credentials are resolved in priority order:
1. `export_aws_credentials` config (default mode) or `export_extra_args` (Ray mode)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. Default credential chain (IAM role, `~/.aws/credentials`)

## Export Row Limit

`export.max_rows` controls the number of rows passed to the export sink. It must be a positive integer. When unset or `null`, export is unlimited.

`export.max_rows_mode` controls the implementation:

- `limit` (default): for default/HuggingFace Dataset export, this is an exact upper bound. For Ray Dataset export, Data-Juicer applies Ray Dataset `limit(max_rows)` before the sink write. The exported row count is bounded by `max_rows`, and Ray may push the limit upstream to reduce work for compatible lazy pipelines. This execution reduction is best effort: operators that need full input, all-to-all operators, filters, materialized datasets, or non-lazy metric collection can still execute more upstream work than `max_rows` rows.
- `quota_reservation`: Ray-only. Data-Juicer inserts a quota actor before the sink, admits whole pyarrow batches until at least `max_rows` rows have been admitted, and materializes the quota-filtered Ray Dataset before the sink write so Ray pre-write schema/sample actions do not consume the quota. If the upstream produces enough rows and the write succeeds, the exported row count is greater than or equal to `max_rows`, and may exceed it by up to one quota batch. Configure that batch granularity with `export.max_rows_quota_batch_size`; larger batches reduce actor calls but can increase overshoot.

`ray_collect_real_metrics: true` is invalid together with `export.max_rows`, because eager Ray Dataset `materialize()`/`count()` before export defeats the lazy limit path.

## Ray File Fan-Out Export

Ray mode can write one processed dataset to multiple HDFS or local directories in a single sink action:

```yaml
executor_type: ray
export:
  targets:
    - target: hdfs
      type: parquet
      path: hdfs://cluster/path/high_score
      mode: overwrite
      filter_condition: "score >= 0.8"
      filesystem: pyarrow
    - target: hdfs
      type: parquet
      path: hdfs://cluster/path/zh
      mode: overwrite
      filter_condition: "lang == 'zh'"
      filesystem: pyarrow
```

Local fan-out uses the same shape:

```yaml
executor_type: ray
export:
  targets:
    - target: local
      type: jsonl
      path: ./outputs/fanout/high_score
      mode: overwrite
      filter_condition: "score >= 0.8"
    - target: local
      type: jsonl
      path: ./outputs/fanout/zh
      mode: overwrite
      filter_condition: "lang == 'zh'"
```

`export.targets` cannot be used together with `export.target`. The first version supports Ray fan-out to `target: hdfs` or `target: local` with `parquet` and `jsonl`; all targets in one list must use the same `target` and `type`. HDFS paths must start with `hdfs://`; local paths can be absolute, relative, or `file://` paths, but the directory must be visible to every Ray worker. Conditions use the same expression syntax as `general_field_filter`. A row can match multiple targets, and any write failure fails the task. `append` is at-least-once, so retries or reruns can produce duplicate part files.

`ray_data_checkpoint.enabled: true` is supported with `export.targets` only when every target explicitly sets `mode: append`. Omitted `mode` still defaults to `error_if_exists` and is rejected in checkpoint fan-out configs. `ray_data_checkpoint.delete_no_checkpoint_files: true` is accepted, but fan-out still uses a custom Ray datasink with post-write checkpointing; it does not provide atomic cleanup across target directories or exactly-once output.

## Stats and Hash Management

During processing, DataJuicer computes intermediate fields:
- **Stats** (`__dj__stats__`, `__dj__meta__`): computed by Filter operators
- **Hashes** (`__dj__hash__`, `__dj__minhash__`, `__dj__simhash__`, etc.): computed by Deduplicator operators

By default, these fields are **removed** from the exported dataset. To keep them:

```yaml
keep_stats_in_res_ds: true                # Keep stats and meta fields
keep_hashes_in_res_ds: true               # Keep hash fields
```

### Stats Export

Regardless of `keep_stats_in_res_ds`, DataJuicer always exports a separate stats file alongside the main dataset:

```
outputs/
├── result.jsonl                          # Main dataset (stats removed by default)
└── result_stats.jsonl                    # Stats-only file (always exported)
```

The stats file contains only the `__dj__stats__` and `__dj__meta__` columns.

## WebDataset Export (Ray Mode)

In Ray mode, you can export to WebDataset format with custom field mapping:

```yaml
export_path: ./outputs/webdataset
export_type: webdataset
export_extra_args:
  field_mapping:
    txt: "text"
    png: "images"
    json: "metadata"
```

## API Reference

### Exporter (Default Mode)

```python
from data_juicer.core.exporter import Exporter

exporter = Exporter(
    export_path="./outputs/result.jsonl",
    export_type="jsonl",
    export_in_parallel=True,
    num_proc=4,
    keep_stats_in_res_ds=False,
    keep_hashes_in_res_ds=False,
)

exporter.export(dataset)
```

### RayExporter (Ray Mode)

```python
from data_juicer.core.ray_exporter import RayExporter

exporter = RayExporter(
    export_path="./outputs/result.jsonl",
    export_type="jsonl",
    keep_stats_in_res_ds=False,
    keep_hashes_in_res_ds=False,
)

exporter.export(ray_dataset)
```

## Troubleshooting

**Export format not supported:**
```bash
# Check supported formats
# Default mode: jsonl, json, parquet
# Ray mode: jsonl, json, parquet, csv, tfrecords, webdataset, lance
```

**Parallel export is slower than expected:**
```yaml
# Disable parallel export
export_in_parallel: false
```

**S3 export fails with permission error:**
```bash
# Verify credentials
aws s3 ls s3://your-bucket/

# Check that export_aws_credentials is configured
```

**Too many output files generated:**
```yaml
# Use supported Ray writer options, when available
export:
  extra_args:
    min_rows_per_file: 200000
```

**Stats missing from exported dataset:**
```yaml
# Keep stats in the result dataset
keep_stats_in_res_ds: true
# Or check the separate stats file: result_stats.jsonl
```
