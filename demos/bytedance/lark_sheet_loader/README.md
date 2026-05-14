# Lark Sheet Loader Demo

This demo shows how to load one Lark Sheet as a Data-Juicer dataset. The loader
exports the sheet as CSV and then uses the normal CSV loading path.

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
