# EcomUserGroupLabel 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `EcomUserGroupLabel`
- `operatorNameCn`: `下单人群和广告人群对比`
- `groupName`: `user_group`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/user_group.py:5`
- DF handler：`metric_ecom_user_group_label`
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
def metric_ecom_user_group_label(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    EcomUserGroupLabel（下单人群和广告人群对比）：
    - 计划ID：读取 ad_material_order_audience_data，找“广告下单人群”中有但“广告触达人群”中没有的
    - 账户ID：遍历主投计划 ad_id 逐个计算并拼接
    返回：消息内容字符串
    """

    def calc_for_ad(ad_id):
        ad_item = None
        material_id_list = []
        diff_list = []
        material_state = state_data.get("material_state")
        for material in material_state:
            if (
                material.get("material_related_ad", "") == ad_id
                and material.get("is_main_material", "") == "是"
            ):
                material_id_list.append(material.get("material_id", ""))

                material_order_audience_data = material.get(
                    "material_order_audience_data", {}
                )
                print(material_order_audience_data)
                reach_dict = material_order_audience_data.get("广告触达人群", {})
                order_dict = material_order_audience_data.get("广告下单人群", {})
                print(reach_dict)
                print(order_dict)
                reach_set = set(reach_dict)
                order_set = set(order_dict)

                diff = list(order_set - reach_set)

                diff_list.append(diff)
        return [material_id_list, diff_list]

    if id_key == "ad_id":
        material_id_list, diff_list = calc_for_ad(id_value)

        details = []
        for material_id, diff in zip(material_id_list, diff_list):
            details.append(
                f"素材ID：{material_id}：素材{material_id} 缺失店铺人群{diff}"
            )

        detail_str = "，".join(details)

        return f"指标名称:八大人群对比, 指标值：{detail_str}"

    if id_key == "adv_id":
        ad_ids = get_main_ad_ids(state_data)
        details_all = []
        for ad_id in ad_ids:
            material_id_list, diff_list = calc_for_ad(ad_id)
            details = []
            for material_id, diff in zip(material_id_list, diff_list):
                details.append(
                    f"素材ID：{material_id}：素材{material_id} 缺失店铺人群{diff}"
                )
            detail_str = "，".join(details)
            details_all.append(f"{detail_str}；")
        return f"指标名称:八大人群对比, 指标值：{''.join(details_all)}"

    raise ValueError(f"Unsupported id for metricCode EcomUserGroupLabel: {id_value}")
```
