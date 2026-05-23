# State 指标计算对齐 Dataset Factory 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标:** 扩展 `StateMetricCalculatorMapper`，让 Data-Juicer 的指标计算算子具备 Dataset Factory 阶段 4 指标计算流程里的关键能力，同时保留当前“从 ADC 接口获取指标元信息和计算口径”的架构。

**架构:** `StateMetricCalculatorMapper` 仍然是唯一算子入口，指标元信息仍然从 `/openapi/state-meta/operators/batch-get` 获取，不迁移 Dataset Factory 的本地 registry。新增一层本地共享 runtime/helper，负责 id 提取、`id_key` 识别、公共数学函数、计算上下文注入和可选 summary 输出；具体指标计算口径仍由接口返回的 `operatorCode` 中的 `calculate(...)` 执行。

**关键假设与前置确认:** 后端 operator detail payload 必须继续提供 `id`、`operatorNameEn`、`operatorNameCn`、`operatorCode`、`inputParameter.params` 等字段；实现前先用现有 mock 或真实接口样例确认字段含义。所有新增输出必须保持 Ray/PyArrow schema 稳定，尤其是嵌套 `metrics.output` 和 `metrics.error` 必须始终是字符串。不能把 Dataset Factory 的指标实现硬复制进 Data-Juicer，Data-Juicer 只维护公共运行时能力和公共数学方法。

**技术栈:** Python 3、Data-Juicer `Mapper`、Ray Dataset、PyArrow schema 稳定性、`PythonScriptRunner` 动态执行可信 Python、ADC OpenAPI `HttpClient`。

---

## 背景和差异

Dataset Factory 阶段 4 的完整流程是：

```text
对每条 sample：
  解析 state
  -> 从 issue_id 提取一个或多个待计算 id
  -> 对每个 id 基于 state_data 识别 id_key（ad_id / adv_id）
  -> 根据 metricCode 路由到具体指标函数
  -> 指标函数使用公共数学函数完成计算并构造 output
  -> 汇总成按 id 分组的 summary JSON 字符串
```

当前 `StateMetricCalculatorMapper` 已具备：

- 从 ADC 接口按 `operator_id` 批量拉取 operator detail。
- 编译并执行每个 operator detail 中的 `operatorCode`。
- 计算入口固定为 `calculate(...)`。
- 根据 `inputParameter.params` 和 `parameter_mapping` 做参数注入。
- 支持一个 sample 中多个 id，输出结构为 `{"id": "...", "items": [...]}`。
- 指标失败不会中断整条样本，会写入 `output: "null"` 和 `error`。

当前主要缺口：

- id 拆分逻辑还没有完全对齐 Dataset Factory 的 `extract_numeric_ids(...)`，尤其是混合字符串中的数字提取和去重。
- 没有公共 `id_key` 识别层，指标代码无法统一知道当前 id 是 `ad_id` 还是 `adv_id`。
- 没有一套 Data-Juicer 内部维护的公共数学函数和公共工具函数。
- `calculate(...)` 只能靠普通参数映射拿数据，缺少 `id_key`、`id_value`、`start_date`、`end_date`、`helpers` 这类公共上下文。
- 没有 Dataset Factory 风格的 summary JSON 字符串输出。
- Dataset Factory 的其他工具执行链路和指标计算不同，本计划暂不纳入。

## 目标行为

保留现有写法：

```python
def calculate(state, ids, bench_roi):
    return 0.82
```

同时支持新的上下文增强写法：

```python
def calculate(state, ids, id_key, start_date=None, end_date=None, helpers=None):
    if id_key == "ad_id":
        return helpers.fmt4(1.23456)
    return "账户口径"
```

兼容原则：

- 函数名仍然只能是 `calculate`。
- 现有 `state`、`inputParameter.params`、`parameter_mapping` 行为不变。
- 新能力通过“保留参数名”按需注入，不要求所有指标都改代码。
- 默认输出结构不变；Dataset Factory summary 作为可选输出字段。

## 不做的事情

- 不复制 Dataset Factory 的 `query_metrics` registry 到 Data-Juicer。
- 不在 Data-Juicer 中维护具体业务指标列表。
- 不实现 `get_industry_creative_tips` 等辅助工具。
- 不改变指标元信息从 ADC 接口获取的事实。
- 不默认替换当前 `query_metric_data_outputs` 的对象结构。

---

### Task 1: 新增指标运行时公共 id helper

**文件:**
- 新增: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- 新增测试: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: 写失败测试**

覆盖 Dataset Factory 中 id 提取和 `id_key` 判断的核心行为：

```python
def test_extract_metric_ids_deduplicates_numeric_fragments():
    assert extract_metric_ids("ad:123, adv:456, again:123") == ["123", "456"]


def test_extract_metric_ids_falls_back_to_stripped_original():
    assert extract_metric_ids("abc_def") == ["abc_def"]


def test_detect_id_key_prefers_ad_when_id_matches_both_ad_and_adv():
    state = {
        "ad_state": [{"ad_id": "123"}],
        "adv_state": [{"adv_id": "123"}],
    }
    assert detect_id_key(state, "123") == "ad_id"


def test_detect_id_key_supports_adv_meta_data_fallback():
    state = {"adv_state": [{"meta_data": {"adv_id": "456"}}]}
    assert detect_id_key(state, "456") == "adv_id"
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

预期：模块或函数不存在，测试失败。

**Step 3: 实现最小 helper**

在 `state_metric_runtime.py` 中实现：

```python
def extract_numeric_ids(value): ...
def extract_metric_ids(value): ...
def detect_id_keys(state_data, id_value): ...
def detect_id_key(state_data, id_value): ...
```

规则：

- `extract_numeric_ids` 对齐 Dataset Factory：纯数字直接返回；混合字符串用 `re.findall(r"\d+")`；按首次出现顺序去重。
- `extract_metric_ids` 在提取不到数字时 fallback 到 `[str(value or "").strip()]`。
- `detect_id_keys` 检查 `ad_state[].ad_id`、`adv_state[].adv_id`、`adv_state[].meta_data.adv_id`。
- `detect_id_key` 在同时命中时优先返回 `ad_id`；只有命中 `adv_id` 且未命中 `ad_id` 时返回 `adv_id`。
- 未命中时返回 `None`，由调用方决定是否写 error。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric runtime id helpers"
```

---

### Task 2: 新增公共数学函数和 helpers 对象

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- 修改测试: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: 写失败测试**

覆盖一小组关键数学能力，不需要一开始把所有函数都测成大矩阵：

```python
def test_helpers_safe_divide_and_parse_percent():
    helpers = MetricHelpers()
    assert helpers.safe_divide(1, 0) == 0.0
    assert helpers.parse_percent_to_ratio("75%") == 0.75


def test_helpers_calc_sequential_stats_integer():
    helpers = MetricHelpers()
    series = {
        "2024-01-01": 10,
        "2024-01-02": 20,
        "2024-01-03": 30,
        "2024-01-04": 60,
    }
    assert helpers.calc_sequential_stats_integer(
        series,
        date(2024, 1, 3),
        date(2024, 1, 4),
    ) == (45, 15, 2.0)
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

预期：`MetricHelpers` 不存在。

**Step 3: 实现 `MetricHelpers`**

从 Dataset Factory `utils/math.py` 迁移纯函数语义，作为 Data-Juicer 自己维护的 helper，不运行时 import Dataset Factory。

首批方法：

- `extract_numeric_values_in_range`
- `sum_numeric_values_in_range`
- `safe_divide`
- `calc_ratio_from_series`
- `calc_sequential_stats`
- `calc_sequential_stats_integer`
- `calc_sequential_stats_for_fraction`
- `calc_bench_compare`
- `fmt4`
- `average`
- `calc_sequential_ratio`
- `parse_percent_to_ratio`
- `resolve_date_range_from_series`
- `parse_duration_seconds`

要求：

- 纯 Python 标准库实现。
- 不依赖外部服务。
- 不改变 Dataset Factory 的口径，除非发现明确 bug 并单独记录。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric math helpers"
```

---

### Task 3: 给 `calculate(...)` 注入公共计算上下文

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

新增配置：

```python
start_date_key: str | None = None
end_date_key: str | None = None
```

新增测试：指标代码可以声明并收到 `id_key`、`id_value`、`start_date`、`end_date`、`helpers`。

```python
def calculate(state, ids, id_key, id_value, start_date=None, end_date=None, helpers=None):
    return {
        "ids": ids,
        "id_key": id_key,
        "id_value": id_value,
        "start": str(start_date),
        "end": str(end_date),
        "fmt": helpers.fmt4(1.2300),
    }
```

样本中放：

```python
{
    "state": {"ad_state": [{"ad_id": "123"}]},
    "issue_id": "123",
    "start": "2024-01-01",
    "end": "2024-01-07",
}
```

预期输出中 `id_key == "ad_id"`，日期被解析为 `datetime.date` 后再被 `str(...)` 成 `2024-01-01` / `2024-01-07`。

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_calculate_can_receive_metric_context
```

预期：当前会报 `inputParameter.params missing parameter: id_key` 或类似错误。

**Step 3: 实现上下文注入**

在 `StateMetricCalculatorMapper` 中：

- 每条 sample 解析 `state` 得到 `state_data`，避免重复解析。
- 每个当前 id 调用 `detect_id_key(state_data, item_id)`。
- 新增 `_resolve_date_value(sample, key)`，支持空值和 `YYYY-MM-DD`。
- 扩展 `_resolve_calculate_args` 的保留参数名：
  - `id_key`
  - `id_value`
  - `start_date`
  - `end_date`
  - `helpers`
- `helpers` 注入 `MetricHelpers()` 实例。

保持现有规则：

- `state` 参数仍从 `state_key` 取。
- 普通参数仍从 `inputParameter.params` 和 `parameter_mapping` 取。
- 默认值参数仍可省略。
- `*args`、`**kwargs`、keyword-only 参数仍不支持。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_calculate_can_receive_metric_context
```

**Step 5: 跑完整 state metric 测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "add state metric calculation context"
```

---

### Task 4: 对齐 Dataset Factory 的多 id 提取逻辑

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

当前算子主要按逗号拆分字符串。新增 mixed string 用例：

```python
def test_output_items_extract_numeric_ids_from_mixed_issue_id(...):
    sample["issue_id"] = "ad:1854751525764108, adv:1853671159428096, again:1854751525764108"
    assert [item["id"] for item in output["items"]] == [
        "1854751525764108",
        "1853671159428096",
    ]
```

新增 fallback 用例：

```python
def test_output_items_fallback_to_original_when_no_numeric_id(...):
    sample["issue_id"] = "abc_def"
    assert output["items"][0]["id"] == "abc_def"
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_output_items_extract_numeric_ids_from_mixed_issue_id
```

预期：当前输出仍带 `ad:` / `adv:` 片段或没有 Dataset Factory 风格去重。

**Step 3: 替换 id 拆分实现**

将 `_split_output_id_value` 改为调用 `state_metric_runtime.extract_metric_ids(value)`。

兼容要求：

- `list` 输入继续保留当前列表语义：每个元素 stringify、strip，过滤空值。
- `str` 输入使用 Dataset Factory 数字提取和 fallback。
- 输出为空时保底 `["unknown"]` 或按当前行为明确保留；如果引入 fallback，则优先与 Dataset Factory 一致。

**Step 4: 跑测试确认通过**

跑新增测试。

**Step 5: 跑 Arrow schema 稳定性测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_metric_failure_output_keeps_arrow_schema_as_string
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "align state metric id extraction"
```

---

### Task 5: 增加可选 Dataset Factory summary 输出

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

新增配置：

```python
summary_output_key: str | None = None
```

当配置该字段时，算子同时写：

- 现有 `output_key` 对象输出。
- 新的 `summary_output_key` 字符串输出。

测试预期：

```python
json.loads(result["metric_summary"]) == {
    "123": {
        "metrics": [
            {"metricCode": "...", "metricName": "...", "output": "..."}
        ]
    }
}
```

失败指标不进入 summary：

- `error` 非空的 metric 跳过。
- `output` 为空的 metric 跳过。
- `output` 包含 `返回调用失败` 的 metric 跳过。

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_summary_output_key_writes_dataset_factory_summary
```

预期：构造函数不接受 `summary_output_key` 或输出字段缺失。

**Step 3: 实现 summary builder**

新增：

```python
def _build_summary_output(metric_outputs: dict[str, Any]) -> str:
    ...
```

规则：

- 遍历 `metric_outputs["items"]`。
- 每个 item 的 `id` 作为 summary map key。
- 只保留成功 metric。
- 当前不写 `tools`。
- 没有任何有效输出时返回空字符串 `""`。
- 默认不启用，避免改变现有输出。

**Step 4: 跑测试确认通过**

跑新增测试。

**Step 5: 跑完整 state metric 测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "add state metric summary output"
```

---

### Task 6: callback config 同步新运行时配置

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

扩展已有 callback config 测试，期望包含：

```python
{
    "start_date_key": None,
    "end_date_key": None,
    "summary_output_key": None,
    "runtime": "adc_operator_code",
}
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_before_operator_started_starts_running_once
```

预期：缺少新字段。

**Step 3: 更新 `_operator_config`**

新增配置字段。

注意：

- 不要把接口拉到的完整 operator detail 放进 callback。
- 不要把 `operatorCode` 放进 callback，避免泄露代码和增大 payload。

**Step 4: 跑测试确认通过**

跑 callback focused test。

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "report state metric runtime config"
```

---

### Task 7: schema 和 Ray 相关验证

**文件:**
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`
- 只有 Ray mapper hook 行为变化时才修改: `tests/core/data/test_ray_dataset.py`

**Step 1: 增加 mixed success/failure schema 测试**

构造生产形状：

- 一个 sample 有两个 id。
- 一个 metric 成功，`output` 是字符串。
- 一个 metric 失败，`output` 是 `"null"`。
- 一个 sample 的 summary 是空字符串。

断言：

- `query_metric_data_outputs.items.metrics.output` 仍然是 `string`。
- `summary_output_key` 对应字段始终是字符串。

**Step 2: 跑 focused schema test**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_dataset_factory_summary_keeps_arrow_schema_stable
```

**Step 3: 跑最终验证命令**

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper

git diff --check
```

**Step 4: 提交**

```bash
git add tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "verify state metric summary schema"
```

---

## 兼容性说明

- 现有 `calculate(...)` 函数继续有效。
- 当前 `output_key` 对象结构继续作为默认输出。
- ADC 接口仍然是指标元信息来源。
- 公共 helper 通过可选保留参数注入，指标代码不声明就不会收到。
- 辅助工具不纳入本次改造；后续如果需要，建议作为单独算子或单独 opt-in 模式设计。

## 最终验证矩阵

实施完成后至少运行：

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper

git diff --check
```

如果本地无法访问 ADC 接口或线上 Ray 环境，不要声称线上完全对齐。只能说明本地 unit/schema 验证通过，最终还需要用真实 `/openapi/state-meta/operators/batch-get` 元信息跑一次 Ray 任务确认。
