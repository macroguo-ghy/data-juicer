# Magnus 导出 `serialize_complex_fields` Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 Magnus 导出新增显式开关 `export.serialize_complex_fields`，在开启 `infer_schema_on_create` 且用户明确选择该开关时，先把顶层复杂字段统一序列化为 JSON 字符串，再进入自动 schema 推断和写表流程，避免空数组/空对象触发不稳定的 Arrow/Magnus 类型推断。

**Architecture:** 保持 `infer_schema_on_create` 的现有语义不变，不隐式修改导出数据。新增一个显式导出策略开关，由 `ExportManager` 把配置透传到 Magnus 导出路径，`write_ray_dataset_to_magnus(...)` / `write_hf_dataset_to_magnus(...)` 在建表和写入前调用共享的复杂字段序列化 helper，把顶层 `dict/list/tuple/set` 列转换成稳定的 `string` 列。Ray 与 HF 两条 Magnus 导出路径都复用同一套规则，确保行为一致、测试集中。

**Critical Assumptions & Early Checks:** 该方案假设当前问题主要来自顶层复杂字段的动态推断不稳定，而不是标量列或显式 schema 投影路径。实现前先用 focused test 证明：不开开关时现有显式 schema / 非推断路径行为不变；开开关时复杂字段会稳定转成字符串；`infer_schema_on_create=false` 时即使配置了开关也不会偷偷改写导出数据。还需要先确认当前 fake Ray/HF dataset 测试基建足以验证“传入 `write_magnus(...)` 前的列类型已变为 string”，避免先改生产代码再补测试桩。

**Tech Stack:** Python 3、Data-Juicer `ExportManager`、`data_juicer/core/io_utils.py`、Ray Dataset、HuggingFace Dataset、PyArrow schema、Magnus `pyiceberg.ray.write_magnus(...)`。

---

## Scope

本计划只覆盖 Magnus 导出链路：

- `export.target: magnus`
- `create_table_if_not_exists`
- `infer_schema_on_create`
- 新增 `serialize_complex_fields`

不做的事情：

- 不修改 Lance/HDFS/JSONL/Parquet 等其他导出目标。
- 不把 `serialize_complex_fields` 绑定成 `infer_schema_on_create=true` 的隐式默认行为。
- 不做递归 schema 建模或自动 `list<object> -> list<struct<...>>` 推断。
- 不改变显式 `export.schema` 的优先级。
- 不在 Python 算子运行阶段提前修改样本；只在导出前处理导出视图。

## Target Behavior

目标配置：

```yaml
export:
  target: magnus
  table_name: data_center.default.demo_output
  create_table_if_not_exists: true
  infer_schema_on_create: true
  serialize_complex_fields: true
```

目标行为：

- 若 `serialize_complex_fields=false` 或缺省，Magnus 导出行为与当前完全一致。
- 若 `serialize_complex_fields=true`：
  - 顶层 `dict/list/tuple/set` 字段在导出前转换成 JSON 字符串。
  - 顶层 `str/int/float/bool/None` 保持不变。
  - 嵌套对象只在其所属顶层字段被整体序列化时一并进入 JSON 字符串，不做列内递归拆列。
  - Ray/HF 导出写入时，传给 Magnus 的对应列类型应为 `string`，不再保留原始嵌套 Arrow 类型。
- 若用户同时提供显式 `export.schema`，仍以显式 schema 为准；该开关只影响导出前的数据形态，不改 `export.schema` 的优先级和投影逻辑。
- 若 `infer_schema_on_create=false`，本期开关不应生效，避免用户以为只是“导出字符串化”却在已有稳定 schema 的表上意外改列类型。

## Design Notes

推荐将开关限制为“仅在 Magnus 自动推断 schema 路径生效”，原因：

- 这次问题来自 `infer_schema_on_create` 的不稳定推断，而不是所有 Magnus 写入都需要字符串化。
- 若对所有 Magnus 导出一刀切，用户会在已有稳定 schema 的表上得到意料之外的 `string` 列。
- 若未来需要扩展成“即使非推断模式也允许统一字符串化”，可以后续再引入更宽泛的导出策略，不应在本次混入。

建议统一在 `io_utils.py` 内新增共享 helper，避免：

- Ray/HF 两条导出路径重复写一套序列化逻辑。
- 只修 Ray 忘了 HF，导致配置语义不一致。

建议序列化规则：

- `dict` -> `json.dumps(dict, ensure_ascii=False, default=str)`
- `list` -> `json.dumps(list, ensure_ascii=False, default=str)`
- `tuple` -> 先转 list，再 JSON 序列化
- `set` -> 先按 `str(x)` 排序转 list，再 JSON 序列化，避免非确定顺序
- 对 JSON 不支持的嵌套对象，使用 `default=str`

不建议在本期支持字段白名单/黑名单；先只做全量复杂字段策略，保持 YAGNI。

---

### Task 1: 为导出配置增加 `serialize_complex_fields` 开关并明确边界

**Files:**
- Modify: `data_juicer/config/config_all.yaml`
- Modify: `data_juicer/core/export_manager.py`
- Test: `tests/core/test_export_manager.py`

**Step 1: Write the failing test**

在 `tests/core/test_export_manager.py` 增加 focused test，断言 Magnus 导出配置会把 `serialize_complex_fields` 透传到 Ray 与 HF writer 调用，但默认值为 `False`：

```python
@patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
def test_magnus_export_passes_serialize_complex_fields_flag_to_ray_writer(self, mock_write):
    ...
    self.assertTrue(mock_write.call_args.kwargs["serialize_complex_fields"])


@patch("data_juicer.core.export_manager.write_hf_dataset_to_magnus")
def test_magnus_export_defaults_serialize_complex_fields_false(self, mock_write):
    ...
    self.assertFalse(mock_write.call_args.kwargs["serialize_complex_fields"])
```

**Step 2: Run test to verify it fails**

Run:

```bash
./.venv/bin/python -m unittest tests.core.test_export_manager.ExportManagerTest.test_magnus_export_passes_serialize_complex_fields_flag_to_ray_writer
./.venv/bin/python -m unittest tests.core.test_export_manager.ExportManagerTest.test_magnus_export_defaults_serialize_complex_fields_false
```

Expected: FAIL because `serialize_complex_fields` is not forwarded yet.

**Step 3: Write minimal implementation**

- 在 `config_all.yaml` 的 `export:` 注释中新增 `serialize_complex_fields` 说明，明确它是 Magnus 自动推断 schema 的显式可选策略，默认 `false`。
- 在 `ExportManager._export_to_magnus(...)` 的三条调用分支中，把：

```python
serialize_complex_fields=self.export_cfg.get("serialize_complex_fields", False)
```

透传给 `write_hf_dataset_to_magnus(...)` / `write_ray_dataset_to_magnus(...)`。

**Step 4: Run test to verify it passes**

Run the same focused tests.

Expected: PASS.

**Step 5: Commit**

```bash
git add data_juicer/config/config_all.yaml data_juicer/core/export_manager.py tests/core/test_export_manager.py
git commit -m "add magnus serialize complex fields export flag"
```

---

### Task 2: 新增共享复杂字段序列化 helper

**Files:**
- Modify: `data_juicer/core/io_utils.py`
- Test: `tests/core/test_io_utils.py`

**Step 1: Write the failing test**

增加纯 helper 测试，固定序列化规则：

```python
def test_serialize_complex_export_value_normalizes_supported_container_types(self):
    assert _serialize_complex_export_value({"a": [1, 2]}, ensure_ascii=False) == '{"a":[1,2]}'
    assert _serialize_complex_export_value((1, 2), ensure_ascii=False) == '[1,2]'
    assert _serialize_complex_export_value({"b", "a"}, ensure_ascii=False) == '["a","b"]'


def test_should_serialize_complex_export_value_only_matches_top_level_containers(self):
    assert _should_serialize_complex_export_value([]) is True
    assert _should_serialize_complex_export_value({}) is True
    assert _should_serialize_complex_export_value("[]") is False
    assert _should_serialize_complex_export_value(1) is False
```

**Step 2: Run test to verify it fails**

Run:

```bash
./.venv/bin/python -m unittest tests.core.test_io_utils.WriteRayDatasetToMagnusTest.test_serialize_complex_export_value_normalizes_supported_container_types
./.venv/bin/python -m unittest tests.core.test_io_utils.WriteRayDatasetToMagnusTest.test_should_serialize_complex_export_value_only_matches_top_level_containers
```

Expected: FAIL because helpers do not exist.

**Step 3: Write minimal implementation**

在 `data_juicer/core/io_utils.py` 中新增共享 helper：

- `_should_serialize_complex_export_value(value)`
- `_normalize_complex_export_value_for_json(value)`
- `_serialize_complex_export_value(value, ensure_ascii=False)`

要求：

- 不修改非复杂顶层值。
- `tuple` 转 `list`。
- `set` 按 `str(x)` 排序后转 `list`。
- `dict` key 统一转成 `str`。
- 最终统一 `json.dumps(..., ensure_ascii=False, default=str, separators=(",", ":"))`，减少无意义空格，保持测试可预测。

**Step 4: Run test to verify it passes**

Run the same focused tests.

Expected: PASS.

**Step 5: Commit**

```bash
git add data_juicer/core/io_utils.py tests/core/test_io_utils.py
git commit -m "add magnus complex field serialization helpers"
```

---

### Task 3: 在 Ray Magnus 导出路径应用复杂字段序列化

**Files:**
- Modify: `data_juicer/core/io_utils.py`
- Test: `tests/core/test_io_utils.py`

**Step 1: Write the failing test**

先覆盖“开开关时，传给 `write_magnus(...)` 的 Ray Dataset schema 变成 string；不开时保持原状”：

```python
def test_write_ray_dataset_to_magnus_serializes_top_level_complex_fields_when_enabled(self):
    dataset = FakeRayDataset([
        {"id": "1", "rubrics": [], "state": {"ok": True}},
    ], pa.schema([
        pa.field("id", pa.string()),
        pa.field("rubrics", pa.list_(pa.null())),
        pa.field("state", pa.struct([pa.field("ok", pa.bool_())])),
    ]))
    ...
    written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
    assert written_dataset._schema.field("rubrics").type == pa.string()
    assert written_dataset._schema.field("state").type == pa.string()


def test_write_ray_dataset_to_magnus_does_not_serialize_complex_fields_when_disabled(self):
    ...
```

再覆盖“只有 `infer_schema_on_create=true` 时才应用”：

```python
def test_write_ray_dataset_to_magnus_ignores_serialize_flag_without_infer_schema_on_create(self):
    ...
```

**Step 2: Run test to verify it fails**

Run:

```bash
./.venv/bin/python -m unittest tests.core.test_io_utils.WriteRayDatasetToMagnusTest.test_write_ray_dataset_to_magnus_serializes_top_level_complex_fields_when_enabled
./.venv/bin/python -m unittest tests.core.test_io_utils.WriteRayDatasetToMagnusTest.test_write_ray_dataset_to_magnus_does_not_serialize_complex_fields_when_disabled
./.venv/bin/python -m unittest tests.core.test_io_utils.WriteRayDatasetToMagnusTest.test_write_ray_dataset_to_magnus_ignores_serialize_flag_without_infer_schema_on_create
```

Expected: FAIL because Ray export path does not rewrite dataset yet.

**Step 3: Write minimal implementation**

在 `write_ray_dataset_to_magnus(...)` 中：

- 读取 `serialize_complex_fields = bool(kwargs.get("serialize_complex_fields", False))`
- 仅当：
  - `serialize_complex_fields is True`
  - `create_table_if_not_exists is True`
  - `infer_schema_on_create is True`
  - `explicit_schema is None`
  
  时应用新逻辑

新增一个 Ray Dataset helper，例如：

- `_serialize_complex_fields_in_ray_dataset(dataset, schema_config=None)`

实现方式：

- 用当前 dataset schema 或 Arrow batch schema 识别顶层复杂列
- 对这些列做一次 `map_batches(batch_format="pyarrow")`
- 把复杂列逐列改写成 JSON string 数组
- 返回新的 Ray Dataset

注意：

- 只处理顶层复杂列，避免递归拆 schema。
- 保持非复杂列原样和原顺序。
- `_MAGNUS_INTERNAL_FIELDS` 不要被误序列化。
- 若显式 `export.schema` 已存在，不走这条逻辑。

**Step 4: Run test to verify it passes**

Run the same focused tests.

Expected: PASS.

**Step 5: Commit**

```bash
git add data_juicer/core/io_utils.py tests/core/test_io_utils.py
git commit -m "serialize ray magnus complex fields on infer schema"
```

---

### Task 4: 在 HF Magnus 导出路径应用相同行为

**Files:**
- Modify: `data_juicer/core/io_utils.py`
- Test: `tests/core/test_io_utils.py`

**Step 1: Write the failing test**

为 `write_hf_dataset_to_magnus(...)` 增加 focused test：

```python
def test_write_hf_dataset_to_magnus_serializes_top_level_complex_fields_when_enabled(self):
    dataset = FakeHFDataset.from_dict({
        "id": ["1"],
        "rubrics": [[]],
        "state": [{"ok": True}],
    })
    ...
```

断言传给 writer 的记录中：

- `rubrics` 是字符串 `"[]"`
- `state` 是字符串 `'{"ok":true}'`

并补一个不开开关时保持原始值的对照测试。

**Step 2: Run test to verify it fails**

Run focused unittest commands for the new HF tests.

Expected: FAIL because HF path has no serialization yet.

**Step 3: Write minimal implementation**

在 `write_hf_dataset_to_magnus(...)` 中复用同一套 helper 逻辑，仅在：

- `serialize_complex_fields=True`
- `create_table_if_not_exists=True`
- `infer_schema_on_create=True`
- `explicit_schema is None`

时，对导出前的 HF dataset 做顶层复杂字段序列化。

若 HF dataset 无法原地高效改写，则只在写入前把每个 batch 记录映射成“复杂列已转字符串”的 `dict` 列表，避免额外大范围重建实现过重。

**Step 4: Run test to verify it passes**

Run the new focused HF tests.

Expected: PASS.

**Step 5: Commit**

```bash
git add data_juicer/core/io_utils.py tests/core/test_io_utils.py
git commit -m "serialize hf magnus complex fields on infer schema"
```

---

### Task 5: 补全端到端回归与文档说明

**Files:**
- Modify: `tests/core/test_export_manager.py`
- Modify: `tests/core/test_io_utils.py`
- Modify: `data_juicer/config/config_all.yaml`
- Optional Modify: `docs/plans/2026-05-22-python-script-mapper-usage.md`

**Step 1: Write the failing regression tests**

补足这几类回归：

- 显式 `export.schema` 存在时，即使开了 `serialize_complex_fields`，也不应走自动字符串化捷径。
- 现有 `infer_schema_on_create` 测试在不开新开关时保持原断言。
- 包含空数组、空对象、非空复杂对象混合的 Ray batch，最终 schema 稳定为 `string`。

**Step 2: Run focused regression tests**

Run:

```bash
./.venv/bin/python -m unittest tests.core.test_export_manager
./.venv/bin/python -m unittest tests.core.test_io_utils
```

Expected: 至少有新旧断言冲突或遗漏失败。

**Step 3: Update docs/comments**

- 在 `config_all.yaml` 注释中明确：
  - 这是 Magnus 自动推断 schema 的显式兼容开关
  - 会把顶层复杂字段降级成 JSON string
  - 默认关闭
- 如需要，在已有 Python Script/Magnus 使用文档里补一句：若 schema 动态且复杂字段经常为空，可优先考虑 `serialize_complex_fields`，显式 schema 仍是更稳定方案。

**Step 4: Run full planned verification**

Run:

```bash
./.venv/bin/python -m unittest tests.core.test_export_manager
./.venv/bin/python -m unittest tests.core.test_io_utils
```

Expected: PASS.

如果本地这些测试依赖内部环境或过慢，至少跑新增用例子集，并在 handoff 中明确未跑范围。

**Step 5: Commit**

```bash
git add tests/core/test_export_manager.py tests/core/test_io_utils.py data_juicer/config/config_all.yaml docs/plans/2026-05-22-python-script-mapper-usage.md
git commit -m "document magnus complex field serialization export option"
```

---

## Verification Notes

- 重点验证“默认关闭时零行为变化”。
- 重点验证 Ray 路径，因本次线上问题出在 `write_ray_dataset_to_magnus(...)`。
- 若 HF 路径没有现成 fake dataset 测试桩，不要为了测试去写很重的集成环境；优先复用现有 fake writer/mock 断言最终写入值。
- 若发现 `map_batches` 序列化后会破坏已存在的 schema 投影/字段顺序，应先修 helper 的列重建方式，再继续推进。

## Open Questions To Resolve During Execution

- 开关是否要限制为 `target: magnus` 且 `infer_schema_on_create=true` 才允许配置，否则仅忽略还是直接报错？
  - 推荐：先忽略，不报错，降低接入成本；但在注释中写清楚只对该路径生效。
- 是否需要跳过少数保留字段，比如已经是 JSON string 的业务列？
  - 推荐：先不做白名单/黑名单；仅根据 Python 顶层类型判断。
- 是否需要把序列化后的字段名做标记，例如新增 `_json` 后缀列？
  - 推荐：不做，保持列名稳定，避免导出 schema 与下游消费双重迁移。

## Suggested Implementation Order

1. 先做配置透传测试与实现。
2. 再做纯 helper 测试。
3. 再落 Ray 路径。
4. 最后补 HF 路径和回归。

Plan complete and saved to `docs/plans/2026-05-27-magnus-serialize-complex-fields-plan.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
