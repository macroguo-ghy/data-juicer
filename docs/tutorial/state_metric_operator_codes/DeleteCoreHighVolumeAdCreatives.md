# DeleteCoreHighVolumeAdCreatives 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `DeleteCoreHighVolumeAdCreatives`
- `operatorNameCn`: `是否删除主要跑量素材`
- `groupName`: `ad_flags`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:45`
- DF handler：`metric_delete_core_high_volume_ad_creatives`
- Data-Juicer 目标入口：`operatorCode.calculate(...)`

## Data-Juicer operatorCode 要求

同步到 ADC 元数据平台时，`operatorCode` 必须定义 `calculate(...)`。推荐签名：

```python
def calculate(state, id_value, id_key, start_date=None, end_date=None, helpers=None):
    ...
```

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
def metric_delete_core_high_volume_ad_creatives(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    DeleteCoreHighVolumeAdCreatives（是否删除主要跑量素材）：
    - 计划ID：取 ad_state[*].is_main_material_deleted
    - 账户ID：遍历主投计划 ad_id，逐个取值拼接
    返回：消息内容字符串
    """

    def get_value(ad_id):
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == ad_id:
                return _to_yes_no(ad.get("is_main_material_deleted"))
        return "否"

    if id_key == "ad_id":
        yes_on = get_value(id_value)
        if yes_on == "是":
            material_list = []
            material_state = state_data.get("material_state", [])
            for material in material_state:
                if (
                    material.get("material_related_ad") == id_value
                    and material.get("is_main_material") == "是"
                    and material.get("material_status") == "已删除"
                ):
                    material_list.append(material.get("material_id"))
            return f"指标名称:是否删除主要跑量素材, 指标值：计划ID:{id_value}：是，素材ID为{material_list}"
        return f"指标名称:是否删除主要跑量素材, 指标值：计划ID:{id_value}：否"
    if id_key == "adv_id":
        ad_ids = get_main_ad_ids(state_data)
        details_all = []
        for ad_id in ad_ids:
            yes_on = get_value(ad_id)
            if yes_on == "是":
                material_list = []
                material_state = state_data.get("material_state", [])
                for material in material_state:
                    if (
                        material.get("material_related_ad") == ad_id
                        and material.get("is_main_material") == "是"
                        and material.get("material_status") == "已删除"
                    ):
                        material_list.append(material.get("material_id"))
                details_all.append(f"计划ID:{ad_id}：是，素材ID为{material_list};")
            else:
                details_all.append(f"计划ID:{ad_id}：否;")
        return f"指标名称:是否删除主要跑量素材, 指标值：{''.join(details_all)}"
    raise ValueError(
        f"Unsupported id for metricCode DeleteCoreHighVolumeAdCreatives: {id_value}"
    )
```
