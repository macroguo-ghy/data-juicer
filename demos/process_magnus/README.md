# Magnus Demo

This demo reads from Magnus table `ghy_test.default.lance_format`, filters rows by
`score >= 85.0`, adds a new `processed_by` column, and writes the result to
`ghy_test.default.lance_format_output3`.

Source sample data:

- `Alice`, `95.5`
- `Bob`, `88.0`
- `Charlie`, `92.3`
- `David`, `75.0`
- `Eve`, `89.9`

Expected output rows after filtering:

- `Alice`, `95.5`, `data_juicer_magnus_demo`
- `Bob`, `88.0`, `data_juicer_magnus_demo`
- `Charlie`, `92.3`, `data_juicer_magnus_demo`
- `Eve`, `89.9`, `data_juicer_magnus_demo`

Run:

```bash
dj-process --config demos/process_magnus/configs/magnus_demo.yaml
```

Config file:

- [configs/magnus_demo.yaml](/Users/bytedance/repo/data-juicer/demos/process_magnus/configs/magnus_demo.yaml)
