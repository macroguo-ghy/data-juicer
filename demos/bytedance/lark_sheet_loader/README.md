# Lark Sheet Loader Demo

This demo shows how to load one Lark Sheet as a Data-Juicer dataset. The loader
exports the sheet as CSV and then uses the normal CSV loading path. If Drive
export is not permitted but sheet values can be read, it falls back to reading
cells and writing the staged CSV.

Before running, replace every placeholder in the YAML files:

- `<spreadsheet_token>`
- `<sheet_id>`
- `<lark_app_id>`
- `<lark_app_secret>`

The source sheet must contain a `text` column. Only `file_extension: csv` is
supported.

Run with the default executor:

```bash
python tools/process_data.py --config demos/bytedance/lark_sheet_loader/lark_sheet_loader_default.yaml
```

Run with local Ray:

```bash
python tools/process_data.py --config demos/bytedance/lark_sheet_loader/lark_sheet_loader_ray.yaml
```

Run a transform and append the processed rows back to the same sheet:

```bash
python tools/process_data.py --config demos/bytedance/lark_sheet_loader/lark_sheet_transform_append.yaml
```

`lark_sheet_transform_append.yaml` uses `text_keys: null`, so it does not
require a `text` column. Its mapper applies the same transformation to every
column in each row: numbers plus 1, strings suffixed with `_process_by_dj`,
empty cells changed to `empty`, and other Python value types left unchanged.
