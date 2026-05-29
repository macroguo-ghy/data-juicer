# adv_violation_info 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `adv_violation_info`
- `operatorNameCn`: `账号是否存在违规`
- `groupName`: `risk`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_violation_info.py:5`
- DF handler：`adv_violation_info`
- Data-Juicer 目标入口：`operatorCode.calculate(...)`

## Data-Juicer operatorCode 要求

同步到 ADC 元数据平台时，`operatorCode` 必须定义 `calculate(...)`。推荐写法：

```python
def calculate(state, id_value, start_date=None, end_date=None, helpers=None):
    id_key = helpers.get_id_key(state, id_value)
    ...
```

其中 `id_value`、`start_date`、`end_date` 需要在 `inputParameter.params` 和 YAML `parameter_mapping` 中显式配置；`id_key` 通过 `helpers.get_id_key(state, id_value)` 获取，不再作为形参注入。

下面保留的是 Dataset Factory 真实计算代码，迁移时需要把 `state_data` 改为 `state`，把公共数学函数改为 `helpers.xxx(...)`，并移除对 Dataset Factory 包路径的运行时依赖。

## DF 模块依赖上下文

```python
from ..query_metric_data import get_main_ad_ids, register_metric
```

## DF 原始计算代码

```python
def adv_violation_info(state_data, tool_input, id_key, id_value, start_date, end_date):
    id_search = id_value
    if id_key == "ad_id":
        for ad_id in state_data.get("ad_state", []):
            if ad_id["ad_id"] == id_value:
                id_search = ad_id.get("related_adv_id", [])

    for adv_id in state_data.get("adv_state", []):
        if adv_id.get("adv_id", []) == id_search:
            return f"指标名称:账户是否违规, 指标值：广告主ID：{id_search}：{adv_id['adv_violation_info']}"
    return f"指标名称:账户是否违规, 指标值：广告主ID：{id_value}：调用失败"
```
