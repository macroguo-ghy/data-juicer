# AdAuditStatusAndMaterialsStatus 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `AdAuditStatusAndMaterialsStatus`
- `operatorNameCn`: `计划及其素材审核状态`
- `groupName`: `ad_flags`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:126`
- DF handler：`metric_ad_audit_status_and_materials_status`
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

def _to_yes_no(value):
    s = str(value).strip()
    if s in ("是", "1", "true", "True", "yes", "YES"):
        return "是"
    if s in ("否", "0", "false", "False", "no", "NO"):
        return "否"
    return "否"
```

## DF 原始计算代码

```python
def metric_ad_audit_status_and_materials_status(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    AdAuditStatusAndMaterialsStatus（计划及其素材审核状态）：
    - 计划ID：取 ad_state[*].is_audit_passed
    - 账户ID：遍历主投计划 ad_id，逐个取值拼接
    返回：消息内容字符串
    """

    def get_value(ad_id):
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == ad_id:
                return str(ad.get("is_audit_passed", "")).strip()
        return ""

    def item(ad_id, v):
        if v == "通过":
            return f"计划ID:{ad_id}：计划：{ad_id} 及其绑定素材均已审核通过"
        return f"计划ID:{ad_id}：计划：{ad_id} 或其绑定素材未审核通过"

    if id_key == "ad_id":
        return f"指标名称:计划或素材是否未过审, 指标值：{item(id_value, get_value(id_value))}"
    if id_key == "adv_id":
        items = [item(ad_id, get_value(ad_id)) for ad_id in get_main_ad_ids(state_data)]
        return f"指标名称:计划或素材是否未过审, 指标值：{'；'.join(items)}"
    raise ValueError(
        f"Unsupported id for metricCode AdAuditStatusAndMaterialsStatus: {id_value}"
    )
```
