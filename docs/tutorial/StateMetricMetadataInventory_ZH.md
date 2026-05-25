# State 指标计算元数据清单

本文档对齐 Data-Juicer 当前 `state_metric_calculator` 算子的元数据口径，用于把 Dataset Factory 中已有的 metric 和 tool 同步到 ADC 后端元数据平台。DF 原始代码拆分保存在 `docs/tutorial/state_metric_operator_codes/`。

## 1. 当前算子元数据口径

`state_metric_calculator` 运行时按 `operators[].operator_id` 调用 `/openapi/state-meta/operators/batch-get` 获取元数据。YAML 只保存选择结果和字段映射，真实名称、入参声明和计算逻辑来自后端元数据。

### 1.1 公共 Data-Juicer YAML 字段

| 字段 | 说明 |
| --- | --- |
| `state_key` | State 所在样本字段，默认 `state`。 |
| `id_source_key` | 样本里的公共 ID 字段，支持逗号分隔多个 ID；summary 顶层 key 和 runtime 注入的 `id_value` 都来自该字段。 |
| `start_date_key` | 样本里的起始日期字段，供 `calculate(..., start_date, ...)` 注入。 |
| `end_date_key` | 样本里的结束日期字段，供 `calculate(..., end_date, ...)` 注入。 |
| `output_key` | 输出字段，默认 `query_metric_data_outputs`。 |
| `result_mode` | 支持 `summary` 和 `object`；两者结构一致，只差 JSON 序列化。 |
| `summary_success_only` | 默认 `false`，保留成功和失败结果以及 `error` 字段。 |

### 1.2 后端 metric 元数据字段

| 字段 | 说明 |
| --- | --- |
| `operatorType` | 固定为 `metric`。 |
| `operatorNameEn` | 输出为 `metrics[].metricCode`。 |
| `operatorNameCn` | 输出为 `metrics[].metricName`。 |
| `groupName` | 建议维护分组，供元数据平台和前端展示。 |
| `inputParameter` | JSON object 或 JSON 字符串，必须包含 `params` 数组。 |
| `operatorCode` | 必须定义 `calculate(...)`。 |

### 1.3 后端 tool 元数据字段

| 字段 | 说明 |
| --- | --- |
| `operatorType` | 固定为 `tool`。 |
| `toolName` | 输出为 `tools[].tool`。 |
| `toolNameCn` | 输出为 `tools[].toolName`。 |
| `groupName` | 建议维护分组，默认可用 `aux_tools`。 |
| `handlerType` / `handlerName` | 当前只作为元信息保留；执行仍以 `operatorCode.calculate(...)` 为准。 |
| `inputParameter` | JSON object 或 JSON 字符串，必须包含 `params` 数组。 |
| `operatorCode` | 必须定义 `calculate(...)`。 |

### 1.4 Runtime 注入参数

这些参数不要写入 `inputParameter.params`，算子会按 `calculate(...)` 签名自动注入：`state`、`id_value`、`id_key`、`start_date`、`end_date`、`helpers`。业务参数才写入 `inputParameter.params` 并通过 YAML `parameter_mapping` 映射。

外部题目或样本字段里的 ID 是主输入来源，通常通过 `id_source_key` 传入；summary 顶层 key 和 `id_value` 都使用这个外部 ID。`id_key` 不是直接取外部字段名，而是用外部 ID 到生成后的 State 里匹配得到：当前只检查 `ad_state[].ad_id`、`adv_state[].adv_id` 和 `adv_state[].meta_data.adv_id`，不会通过 `ad_state[].related_adv_id` 推断 `adv_id`。

## 2. Metric 清单

| 分组 | operatorNameEn | operatorNameCn | inputParameter 建议 | 代码文档 | DF 来源 |
| --- | --- | --- | --- | --- | --- |
| `materials` | `AdOnlineMaterialsCount` | 在投素材数环比 | `{"params":[]}` | [AdOnlineMaterialsCount](state_metric_operator_codes/AdOnlineMaterialsCount.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:117` |
| `materials` | `DeclineMaterialsCount` | 衰退素材数 | `{"params":[]}` | [DeclineMaterialsCount](state_metric_operator_codes/DeclineMaterialsCount.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:168` |
| `materials` | `SameRiskMaterialsCount` | 同质化素材数 | `{"params":[]}` | [SameRiskMaterialsCount](state_metric_operator_codes/SameRiskMaterialsCount.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:197` |
| `materials` | `sequential_3s_rate` | 素材3s完播率环比 | `{"params":[]}` | [sequential_3s_rate](state_metric_operator_codes/sequential_3s_rate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:440` |
| `materials` | `AdOnlineMaterialsCountBench` | 在投素材数同行数据 | `{"params":[]}` | [AdOnlineMaterialsCountBench](state_metric_operator_codes/AdOnlineMaterialsCountBench.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:549` |
| `materials` | `bench_3s_rate` | 素材3s完播率同行数据 | `{"params":[]}` | [bench_3s_rate](state_metric_operator_codes/bench_3s_rate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:499` |
| `materials` | `sequential_cvr` | 素材 CVR（环比） | `{"params":[]}` | [sequential_cvr](state_metric_operator_codes/sequential_cvr.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:333` |
| `materials` | `bench_cvr` | 素材 CVR（同行） | `{"params":[]}` | [bench_cvr](state_metric_operator_codes/bench_cvr.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:392` |
| `materials` | `sequential_ctr` | 素材 CTR（环比） | `{"params":[]}` | [sequential_ctr](state_metric_operator_codes/sequential_ctr.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:226` |
| `materials` | `bench_ctr` | 素材 CTR（同行） | `{"params":[]}` | [bench_ctr](state_metric_operator_codes/bench_ctr.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:285` |
| `materials` | `LowEfficiencyMaterialsCount` | 低效素材数 | `{"params":[]}` | [LowEfficiencyMaterialsCount](state_metric_operator_codes/LowEfficiencyMaterialsCount.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:75` |
| `materials` | `merchant_score` | 店铺体验分 | `{"params":[]}` | [merchant_score](state_metric_operator_codes/merchant_score.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:37` |
| `live` | `LiveWatchRateRange` | 直播观看点击率环比 | `{"params":[]}` | [LiveWatchRateRange](state_metric_operator_codes/LiveWatchRateRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:86` |
| `live` | `LiveProductClickRate` | 直播商品点击率同行数据 | `{"params":[]}` | [LiveProductClickRate](state_metric_operator_codes/LiveProductClickRate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:280` |
| `live` | `LiveDORateRange` | 直播点击支付率环比 | `{"params":[]}` | [LiveDORateRange](state_metric_operator_codes/LiveDORateRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:204` |
| `live` | `LiveDORate` | 直播点击支付率同行数据 | `{"params":[]}` | [LiveDORate](state_metric_operator_codes/LiveDORate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:314` |
| `live` | `LiveProductClickRateRange` | 直播商品点击率环比 | `{"params":[]}` | [LiveProductClickRateRange](state_metric_operator_codes/LiveProductClickRateRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:124` |
| `live` | `LiveWatchRate` | 直播观看点击率同行数据 | `{"params":[]}` | [LiveWatchRate](state_metric_operator_codes/LiveWatchRate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:348` |
| `live` | `LiveProductShowRate` | 直播商品曝光率同行数据 | `{"params":[]}` | [LiveProductShowRate](state_metric_operator_codes/LiveProductShowRate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:31` |
| `live` | `LiveProductShowRateRange` | 直播商品曝光率环比 | `{"params":[]}` | [LiveProductShowRateRange](state_metric_operator_codes/LiveProductShowRateRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:164` |
| `live` | `TotalConvertRateRange` | 直播整体转化率环比 | `{"params":[]}` | [TotalConvertRateRange](state_metric_operator_codes/TotalConvertRateRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:242` |
| `live` | `TotalConvertRate` | 直播整体转化率同行数据 | `{"params":[]}` | [TotalConvertRate](state_metric_operator_codes/TotalConvertRate.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:382` |
| `live` | `AvgStayTimeRange` | 直播平均停留时长环比 | `{"params":[]}` | [AvgStayTimeRange](state_metric_operator_codes/AvgStayTimeRange.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:416` |
| `live` | `AvgStayTime` | 直播平均停留时长同行数据 | `{"params":[]}` | [AvgStayTime](state_metric_operator_codes/AvgStayTime.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:458` |
| `roi` | `GetAdROIAndRecommendedValues` | 当前ROI对比推荐ROI | `{"params":[]}` | [GetAdROIAndRecommendedValues](state_metric_operator_codes/GetAdROIAndRecommendedValues.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:8` |
| `roi` | `AdROIBench` | ROI同行数据 | `{"params":[]}` | [AdROIBench](state_metric_operator_codes/AdROIBench.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:107` |
| `roi` | `sequential_roi` | ROI数据 | `{"params":[]}` | [sequential_roi](state_metric_operator_codes/sequential_roi.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:58` |
| `ad_flags` | `BidAdjustmentTimes` | 是否频繁调整出价 | `{"params":[]}` | [BidAdjustmentTimes](state_metric_operator_codes/BidAdjustmentTimes.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:14` |
| `ad_flags` | `DeleteCoreHighVolumeAdCreatives` | 是否删除主要跑量素材 | `{"params":[]}` | [DeleteCoreHighVolumeAdCreatives](state_metric_operator_codes/DeleteCoreHighVolumeAdCreatives.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:45` |
| `ad_flags` | `AdStatus` | 计划是否暂停 | `{"params":[]}` | [AdStatus](state_metric_operator_codes/AdStatus.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:100` |
| `ad_flags` | `AdAuditStatusAndMaterialsStatus` | 计划及其素材审核状态 | `{"params":[]}` | [AdAuditStatusAndMaterialsStatus](state_metric_operator_codes/AdAuditStatusAndMaterialsStatus.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:126` |
| `ad_flags` | `PlanMarGoalIsVideoPromGoods` | 是否为直播计划 | `{"params":[]}` | [PlanMarGoalIsVideoPromGoods](state_metric_operator_codes/PlanMarGoalIsVideoPromGoods.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:158` |
| `user_group` | `EcomUserGroupLabel` | 下单人群和广告人群对比 | `{"params":[]}` | [EcomUserGroupLabel](state_metric_operator_codes/EcomUserGroupLabel.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/user_group.py:5` |
| `ecp` | `ecp_pc_cost_ratio` | 千川PC消耗占比 | `{"params":[]}` | [ecp_pc_cost_ratio](state_metric_operator_codes/ecp_pc_cost_ratio.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:61` |
| `ecp` | `ecp_effective_cost` | 千川有效消耗门槛 | `{"params":[]}` | [ecp_effective_cost](state_metric_operator_codes/ecp_effective_cost.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:75` |
| `ecp` | `ecp_balance_enough` | 账户余额是否充足 | `{"params":[]}` | [ecp_balance_enough](state_metric_operator_codes/ecp_balance_enough.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:114` |
| `cost` | `adv_cost_ratio` | 账户消耗环比 | `{"params":[]}` | [adv_cost_ratio](state_metric_operator_codes/adv_cost_ratio.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_cost_ratio.py:6` |
| `cost` | `ad_cost_ratio` | 计划消耗环比 | `{"params":[]}` | [ad_cost_ratio](state_metric_operator_codes/ad_cost_ratio.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_cost_ratio.py:6` |
| `risk` | `adv_violation_info` | 账号是否存在违规 | `{"params":[]}` | [adv_violation_info](state_metric_operator_codes/adv_violation_info.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_violation_info.py:5` |

## 3. Tool 清单

| 分组 | toolName | toolNameCn | handlerType | handlerName | inputParameter 建议 | 代码文档 | DF 来源 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `aux_tools` | `customer_info_acquisition` | 客户信息获取 | `builtin` | `customer_info_acquisition` | `{"params":[]}` | [customer_info_acquisition](state_metric_operator_codes/customer_info_acquisition.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/customer_info_acquisition.py:7` |
| `aux_tools` | `get_industry_creative_tips` | 行业创意建议 | `builtin` | `get_industry_creative_tips` | `{"params":[]}` | [get_industry_creative_tips](state_metric_operator_codes/get_industry_creative_tips.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/get_industry_creative_tips.py:7` |
| `aux_tools` | `send_insight_report` | 发送洞察报告 | `builtin` | `send_insight_report` | `{"params":[]}` | [send_insight_report](state_metric_operator_codes/send_insight_report.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/send_insight_report.py:5` |
| `aux_tools` | `ad_plan_diagnosis_sec` | 广告计划诊断 | `builtin` | `ad_plan_diagnosis_sec` | `{"params":[]}` | [ad_plan_diagnosis_sec](state_metric_operator_codes/ad_plan_diagnosis_sec.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/ad_plan_diagnosis_sec.py:7` |
| `aux_tools` | `retrieve_id_type` | 识别ID类型 | `builtin` | `retrieve_id_type` | `{"params":[]}` | [retrieve_id_type](state_metric_operator_codes/retrieve_id_type.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/retrieve_id_type.py:6` |
| `aux_tools` | `review_exception_diagnosis_and_promption` | 审核异常诊断与申诉 | `builtin` | `review_exception_diagnosis_and_promption` | `{"params":[]}` | [review_exception_diagnosis_and_promption](state_metric_operator_codes/review_exception_diagnosis_and_promption.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/review_exception_diagnosis_and_promption.py:5` |
| `aux_tools` | `get_douplus_order_detail` | 订单详情查询 | `builtin` | `get_douplus_order_detail` | `{"params":[]}` | [get_douplus_order_detail](state_metric_operator_codes/get_douplus_order_detail.md) | `/Users/bytedance/develop/dataset_factory/tool_handlers/get_douplus_order_detail.py:5` |

## 4. Metric 口径摘要

下面的口径摘要来自 Dataset Factory 清单，但字段命名按 Data-Juicer 当前元数据模型补充。迁移到 ADC 元数据平台时，以本节的 `operatorNameEn`、`operatorNameCn`、`groupName` 和代码文档为准。

### 素材类指标

#### `AdOnlineMaterialsCount`
- `operatorType`: `metric`
- `operatorNameCn`: 在投素材数环比
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AdOnlineMaterialsCount](state_metric_operator_codes/AdOnlineMaterialsCount.md)

DF 口径摘要：
- 中文名：在投素材数环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:116)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_active_materials_count`
  - `adv_state[].adv_active_materials_count`
- 计算口径：
  - 用 `calc_sequential_stats_integer(series, start_date, end_date)`
  - 计算本周期均值、上周期均值、环比
  - `ad_id` 和 `adv_id` 各自按对应序列计算

#### `DeclineMaterialsCount`
- `operatorType`: `metric`
- `operatorNameCn`: 衰退素材数
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[DeclineMaterialsCount](state_metric_operator_codes/DeclineMaterialsCount.md)

DF 口径摘要：
- 中文名：衰退素材数
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:167)
- 外部输入字段：`state`, `id`
- `state` 依赖字段：
  - `ad_state[].ad_declining_materials_count`
  - `adv_state[].adv_declining_materials_count`
- 计算口径：直接取对应字段

#### `SameRiskMaterialsCount`
- `operatorType`: `metric`
- `operatorNameCn`: 同质化素材数
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[SameRiskMaterialsCount](state_metric_operator_codes/SameRiskMaterialsCount.md)

DF 口径摘要：
- 中文名：同质化素材数
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:196)
- 外部输入字段：`state`, `id`
- `state` 依赖字段：
  - `ad_state[].ad_homogeneous_materials_count`
  - `adv_state[].adv_homogeneous_materials_count`
- 计算口径：直接取对应字段

#### `sequential_3s_rate`
- `operatorType`: `metric`
- `operatorNameCn`: 素材3s完播率环比
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[sequential_3s_rate](state_metric_operator_codes/sequential_3s_rate.md)

DF 口径摘要：
- 中文名：素材3s完播率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:439)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_3s_completion_materials_count`
  - `ad_state[].ad_material_impressions`
  - `adv_state[].adv_3s_completion_materials_count`
  - `adv_state[].adv_material_impressions`
- 计算口径：
  - 用 `calc_sequential_stats_for_fraction(3s_count, impressions, start_date, end_date)`

#### `AdOnlineMaterialsCountBench`
- `operatorType`: `metric`
- `operatorNameCn`: 在投素材数同行数据
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AdOnlineMaterialsCountBench](state_metric_operator_codes/AdOnlineMaterialsCountBench.md)

DF 口径摘要：
- 中文名：在投素材数同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:548)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_active_materials_count`
  - `adv_state[].adv_active_materials_count`
  - `world_state.bench_active_materials_count`
- 计算口径：
  - 当前值用 `calc_sequential_stats_integer(...)[0]` 获取本周期均值
  - 同行基准来自 `bench_active_materials_count`
  - 用 `calc_bench_compare`

### 3.2 直播类指标

#### `bench_3s_rate`
- `operatorType`: `metric`
- `operatorNameCn`: 素材3s完播率同行数据
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[bench_3s_rate](state_metric_operator_codes/bench_3s_rate.md)

DF 口径摘要：
- 中文名：素材3s完播率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:498)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_3s_completion_materials_count`
  - `ad_state[].ad_material_impressions`
  - `adv_state[].adv_3s_completion_materials_count`
  - `adv_state[].adv_material_impressions`
  - `world_state.bench_3s_completion_rate`
- 计算口径：
  - 当前 3s 完播率用 `calc_ratio_from_series`
  - 同行基准来自 `bench_3s_completion_rate`
  - 用 `calc_bench_compare`

#### `sequential_cvr`
- `operatorType`: `metric`
- `operatorNameCn`: 素材 CVR（环比）
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[sequential_cvr](state_metric_operator_codes/sequential_cvr.md)

DF 口径摘要：
- 中文名：素材 CVR（环比）
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:332)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_material_conversions`
  - `ad_state[].ad_material_clicks`
  - `adv_state[].adv_material_conversions`
  - `adv_state[].adv_material_clicks`
- 计算口径：
  - 用 `calc_sequential_stats_for_fraction(conversions, clicks, start_date, end_date)`

#### `bench_cvr`
- `operatorType`: `metric`
- `operatorNameCn`: 素材 CVR（同行）
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[bench_cvr](state_metric_operator_codes/bench_cvr.md)

DF 口径摘要：
- 中文名：素材 CVR（同行）
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:391)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_material_conversions`
  - `ad_state[].ad_material_clicks`
  - `adv_state[].adv_material_conversions`
  - `adv_state[].adv_material_clicks`
  - `world_state.bench_material_cvr`
- 计算口径：
  - 当前 CVR 用 `calc_ratio_from_series`
  - 同行基准来自 `bench_material_cvr`
  - 用 `calc_bench_compare`

#### `sequential_ctr`
- `operatorType`: `metric`
- `operatorNameCn`: 素材 CTR（环比）
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[sequential_ctr](state_metric_operator_codes/sequential_ctr.md)

DF 口径摘要：
- 中文名：素材 CTR（环比）
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:225)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_material_clicks`
  - `ad_state[].ad_material_impressions`
  - `adv_state[].adv_material_clicks`
  - `adv_state[].adv_material_impressions`
- 计算口径：
  - 用 `calc_sequential_stats_for_fraction(clicks, impressions, start_date, end_date)`
  - 先算比例，再算本周期/上周期环比

#### `bench_ctr`
- `operatorType`: `metric`
- `operatorNameCn`: 素材 CTR（同行）
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[bench_ctr](state_metric_operator_codes/bench_ctr.md)

DF 口径摘要：
- 中文名：素材 CTR（同行）
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:284)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_material_clicks`
  - `ad_state[].ad_material_impressions`
  - `adv_state[].adv_material_clicks`
  - `adv_state[].adv_material_impressions`
  - `world_state.bench_material_ctr`
- 计算口径：
  - 用 `calc_ratio_from_series` 算当前 CTR
  - 用 `parse_percent_to_ratio(world_state.bench_material_ctr)` 取同行基准
  - 用 `calc_bench_compare(cur, bench)` 比较高低和差值

#### `LowEfficiencyMaterialsCount`
- `operatorType`: `metric`
- `operatorNameCn`: 低效素材数
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LowEfficiencyMaterialsCount](state_metric_operator_codes/LowEfficiencyMaterialsCount.md)

DF 口径摘要：
- 中文名：低效素材数
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:74)
- 外部输入字段：`state`, `id`
- `state` 依赖字段：
  - `ad_state[].ad_inefficient_materials_count`
  - `adv_state[].adv_inefficient_materials_count`
  - 兼容 `adv_state[].adv_metrics.adv_inefficient_materials_count`
- 计算口径：直接取对应字段，不做时间序列计算

#### `merchant_score`
- `operatorType`: `metric`
- `operatorNameCn`: 店铺体验分
- `groupName`: `materials`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[merchant_score](state_metric_operator_codes/merchant_score.md)

DF 口径摘要：
- 中文名：店铺体验分
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:36)
- 外部输入字段：`state`, `id`
- `state` 依赖字段：
  - `ad_state[].ad_id`
  - `ad_state[].shop_experience_score`
  - `ad_state[].is_main_ad`（`adv_id` 口径时通过 `get_main_ad_ids` 使用）
- 计算口径：
  - `ad_id`：直接返回该计划的 `shop_experience_score`
  - `adv_id`：遍历主投计划，逐个返回

### 直播类指标

#### `LiveWatchRateRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播观看点击率环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveWatchRateRange](state_metric_operator_codes/LiveWatchRateRange.md)

DF 口径摘要：
- 中文名：直播观看点击率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:85)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_live_watch_count`
  - `ad_state[].ad_live_impressions`
  - `ad_state[].is_main_ad`（`adv_id` 口径时）
- 计算口径：
  - 用 `calc_sequential_stats_for_fraction(watch_count, impressions, start_date, end_date)`

#### `LiveProductClickRate`
- `operatorType`: `metric`
- `operatorNameCn`: 直播商品点击率同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveProductClickRate](state_metric_operator_codes/LiveProductClickRate.md)

DF 口径摘要：
- 中文名：直播商品点击率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:279)
- 依赖字段：
  - `ad_live_product_clicks`
  - `ad_live_product_impressions`
  - `world_state.bench_product_click_rate`
  - `is_main_ad`
- 计算口径：当前点击率 vs 同行基准

#### `LiveDORateRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播点击支付率环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveDORateRange](state_metric_operator_codes/LiveDORateRange.md)

DF 口径摘要：
- 中文名：直播点击支付率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:203)
- 依赖字段：
  - `ad_live_product_orders`
  - `ad_live_product_clicks`
  - `is_main_ad`
- 计算口径：`orders / product_clicks` 的环比

#### `LiveDORate`
- `operatorType`: `metric`
- `operatorNameCn`: 直播点击支付率同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveDORate](state_metric_operator_codes/LiveDORate.md)

DF 口径摘要：
- 中文名：直播点击支付率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:313)
- 依赖字段：
  - `ad_live_product_orders`
  - `ad_live_product_clicks`
  - `world_state.bench_click_to_pay_rate`
  - `is_main_ad`
- 计算口径：当前点击支付率 vs 同行基准

#### `LiveProductClickRateRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播商品点击率环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveProductClickRateRange](state_metric_operator_codes/LiveProductClickRateRange.md)

DF 口径摘要：
- 中文名：直播商品点击率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:123)
- 依赖字段：
  - `ad_live_product_clicks`
  - `ad_live_product_impressions`
  - `is_main_ad`（`adv_id` 口径）
- 计算口径：`clicks / impressions` 的环比

#### `LiveWatchRate`
- `operatorType`: `metric`
- `operatorNameCn`: 直播观看点击率同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveWatchRate](state_metric_operator_codes/LiveWatchRate.md)

DF 口径摘要：
- 中文名：直播观看点击率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:347)
- 依赖字段：
  - `ad_live_watch_count`
  - `ad_live_impressions`
  - `world_state.bench_watch_to_click_rate`
  - `is_main_ad`
- 计算口径：当前观看点击率 vs 同行基准

#### `LiveProductShowRate`
- `operatorType`: `metric`
- `operatorNameCn`: 直播商品曝光率同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveProductShowRate](state_metric_operator_codes/LiveProductShowRate.md)

DF 口径摘要：
- 中文名：直播商品曝光率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:30)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- `state` 依赖字段：
  - `ad_state[].ad_live_product_impressions`
  - `ad_state[].ad_live_watch_count`
  - `ad_state[].is_main_ad`（`adv_id` 口径时）
  - `world_state.bench_product_impression_rate`
- 计算口径：
  - `商品曝光率 = 区间内商品曝光总和 / 区间内观看总和`
  - 同行基准来自 `bench_product_impression_rate`
  - `adv_id` 口径遍历主投计划逐个输出

#### `LiveProductShowRateRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播商品曝光率环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[LiveProductShowRateRange](state_metric_operator_codes/LiveProductShowRateRange.md)

DF 口径摘要：
- 中文名：直播商品曝光率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:163)
- 依赖字段：
  - `ad_live_product_impressions`
  - `ad_live_watch_count`
  - `is_main_ad`
- 计算口径：`product_impressions / watch_count` 的环比

#### `TotalConvertRateRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播整体转化率环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[TotalConvertRateRange](state_metric_operator_codes/TotalConvertRateRange.md)

DF 口径摘要：
- 中文名：直播整体转化率环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:241)
- 依赖字段：
  - `ad_live_product_orders`
  - `ad_live_impressions`
  - `is_main_ad`
- 计算口径：`orders / impressions` 的环比

#### `TotalConvertRate`
- `operatorType`: `metric`
- `operatorNameCn`: 直播整体转化率同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[TotalConvertRate](state_metric_operator_codes/TotalConvertRate.md)

DF 口径摘要：
- 中文名：直播整体转化率同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:381)
- 依赖字段：
  - `ad_live_product_orders`
  - `ad_live_impressions`
  - `world_state.bench_live_conversion_rate`
  - `is_main_ad`
- 计算口径：当前整体转化率 vs 同行基准

#### `AvgStayTimeRange`
- `operatorType`: `metric`
- `operatorNameCn`: 直播平均停留时长环比
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AvgStayTimeRange](state_metric_operator_codes/AvgStayTimeRange.md)

DF 口径摘要：
- 中文名：直播平均停留时长环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:415)
- 依赖字段：
  - `ad_live_total_stay_duration`
  - `ad_live_viewer_count`
  - `is_main_ad`
- 计算口径：
  - 用 `calc_sequential_stats_for_fraction(total_stay_duration, viewer_count, ...)`
  - 输出按秒展示

#### `AvgStayTime`
- `operatorType`: `metric`
- `operatorNameCn`: 直播平均停留时长同行数据
- `groupName`: `live`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AvgStayTime](state_metric_operator_codes/AvgStayTime.md)

DF 口径摘要：
- 中文名：直播平均停留时长同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:457)
- 依赖字段：
  - `ad_live_total_stay_duration`
  - `ad_live_viewer_count`
  - `world_state.bench_avg_stay_duration`
  - `is_main_ad`
- 计算口径：
  - 当前停留时长用 `total_stay_duration / viewer_count`
  - 同行基准通过 `parse_duration_seconds(bench_avg_stay_duration)` 解析
  - 用 `calc_bench_compare`

### 3.3 ROI 类指标

### ROI 类指标

#### `GetAdROIAndRecommendedValues`
- `operatorType`: `metric`
- `operatorNameCn`: 当前ROI对比推荐ROI
- `groupName`: `roi`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[GetAdROIAndRecommendedValues](state_metric_operator_codes/GetAdROIAndRecommendedValues.md)

DF 口径摘要：
- 中文名：当前ROI对比推荐ROI
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:7)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `ad_state[].ad_id`
  - `ad_state[].customer_set_roi`
  - `ad_state[].sys_recommended_roi`
  - `ad_state[].is_main_ad`（`adv_id` 口径）
- 计算口径：
  - 以 `customer_set_roi` 作为当前值
  - 与 `sys_recommended_roi` 做差值百分比比较
  - `adv_id` 口径遍历主投计划逐个输出

#### `AdROIBench`
- `operatorType`: `metric`
- `operatorNameCn`: ROI同行数据
- `groupName`: `roi`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AdROIBench](state_metric_operator_codes/AdROIBench.md)

DF 口径摘要：
- 中文名：ROI同行数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:106)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- 依赖字段：
  - `ad_state[].ad_roi`
  - `adv_state[].adv_roi`
  - `world_state.bench_roi`
- 计算口径：
  - 先算当前 ROI 在区间内的均值
  - 再与 `bench_roi` 比较
  - `bench_roi` 若是列表，则按分位比较；若是标量，则按数值比较

### 3.4 标志类指标

#### `sequential_roi`
- `operatorType`: `metric`
- `operatorNameCn`: ROI数据
- `groupName`: `roi`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[sequential_roi](state_metric_operator_codes/sequential_roi.md)

DF 口径摘要：
- 中文名：ROI数据
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:57)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- 依赖字段：
  - `ad_state[].ad_roi`
  - `adv_state[].adv_roi`
- 计算口径：
  - 用 `calc_sequential_ratio(series_map, start_date, end_date)`
  - 输出上周期均值、本周期均值及环比

### 标志类指标

#### `BidAdjustmentTimes`
- `operatorType`: `metric`
- `operatorNameCn`: 是否频繁调整出价
- `groupName`: `ad_flags`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[BidAdjustmentTimes](state_metric_operator_codes/BidAdjustmentTimes.md)

DF 口径摘要：
- 中文名：是否频繁调整出价
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:13)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `ad_state[].is_bid_frequently_adjusted`
  - `ad_state[].is_main_ad`
- 计算口径：取“是/否”标志；`adv_id` 口径遍历主投计划

#### `DeleteCoreHighVolumeAdCreatives`
- `operatorType`: `metric`
- `operatorNameCn`: 是否删除主要跑量素材
- `groupName`: `ad_flags`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[DeleteCoreHighVolumeAdCreatives](state_metric_operator_codes/DeleteCoreHighVolumeAdCreatives.md)

DF 口径摘要：
- 中文名：是否删除主要跑量素材
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:44)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `ad_state[].is_main_material_deleted`
  - `material_state[].material_related_ad`
  - `material_state[].is_main_material`
  - `material_state[].material_status`
  - `material_state[].material_id`
  - `ad_state[].is_main_ad`
- 计算口径：
  - 若计划标记为删除主素材，则进一步从 `material_state` 找主素材且状态为“已删除”的素材 ID

#### `AdStatus`
- `operatorType`: `metric`
- `operatorNameCn`: 计划是否暂停
- `groupName`: `ad_flags`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AdStatus](state_metric_operator_codes/AdStatus.md)

DF 口径摘要：
- 中文名：计划是否暂停
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:99)
- 依赖字段：
  - `ad_state[].is_ad_paused`
  - `ad_state[].is_main_ad`
- 计算口径：布尔/枚举转“是/否”

#### `AdAuditStatusAndMaterialsStatus`
- `operatorType`: `metric`
- `operatorNameCn`: 计划及其素材审核状态
- `groupName`: `ad_flags`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[AdAuditStatusAndMaterialsStatus](state_metric_operator_codes/AdAuditStatusAndMaterialsStatus.md)

DF 口径摘要：
- 中文名：计划及其素材审核状态
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:125)
- 依赖字段：
  - `ad_state[].is_audit_passed`
  - `ad_state[].is_main_ad`
- 计算口径：
  - 若值为“通过”，输出“计划及其绑定素材均已审核通过”
  - 否则输出未通过

#### `PlanMarGoalIsVideoPromGoods`
- `operatorType`: `metric`
- `operatorNameCn`: 是否为直播计划
- `groupName`: `ad_flags`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[PlanMarGoalIsVideoPromGoods](state_metric_operator_codes/PlanMarGoalIsVideoPromGoods.md)

DF 口径摘要：
- 中文名：是否为直播计划
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:157)
- 依赖字段：
  - `ad_state[].is_live_ad`
  - `ad_state[].is_main_ad`
- 计算口径：布尔/枚举转“是/否”

### 3.5 人群类指标

### 人群类指标

#### `EcomUserGroupLabel`
- `operatorType`: `metric`
- `operatorNameCn`: 下单人群和广告人群对比
- `groupName`: `user_group`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[EcomUserGroupLabel](state_metric_operator_codes/EcomUserGroupLabel.md)

DF 口径摘要：
- 中文名：下单人群和广告人群对比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/user_group.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/user_group.py:4)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `material_state[].material_related_ad`
  - `material_state[].is_main_material`
  - `material_state[].material_id`
  - `material_state[].material_order_audience_data`
  - `ad_state[].is_main_ad`
- 计算口径：
  - 对每个主素材，比较“广告下单人群”与“广告触达人群”
  - 输出只在下单人群中出现、但未在触达人群中出现的人群差集

### 3.6 ECP / 消耗 / 违规类指标

### ECP 类指标

#### `ecp_pc_cost_ratio`
- `operatorType`: `metric`
- `operatorNameCn`: 千川PC消耗占比
- `groupName`: `ecp`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[ecp_pc_cost_ratio](state_metric_operator_codes/ecp_pc_cost_ratio.md)

DF 口径摘要：
- 中文名：千川PC消耗占比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:60)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `adv_state[].adv_id`
  - `adv_state[].ecp_pc_cost_ratio`
- 计算口径：
  - 仅支持 `adv_id`
  - 直接返回账户字段值

#### `ecp_effective_cost`
- `operatorType`: `metric`
- `operatorNameCn`: 千川有效消耗门槛
- `groupName`: `ecp`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[ecp_effective_cost](state_metric_operator_codes/ecp_effective_cost.md)

DF 口径摘要：
- 中文名：千川有效消耗门槛
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:74)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- 依赖字段：
  - `ad_state[].ad_gmv`
  - `ad_state[].ad_pay_counts`
  - `adv_state[].adv_gmv`
  - `adv_state[].adv_pay_counts`
- 计算口径：
  - 先对区间内 GMV、支付次数做 `calc_sequential_ratio(...)[1]` 取本周期累计/结果
  - 再计算 `3 * GMV / 支付次数`

#### `ecp_balance_enough`
- `operatorType`: `metric`
- `operatorNameCn`: 账户余额是否充足
- `groupName`: `ecp`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[ecp_balance_enough](state_metric_operator_codes/ecp_balance_enough.md)

DF 口径摘要：
- 中文名：账户余额是否充足
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:113)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `ad_state[].related_adv_id`
  - `adv_state[].adv_id`
  - `adv_state[].is_low_adv_balance`
- 计算口径：
  - `ad_id` 口径先通过 `related_adv_id` 找到账户
  - 最终返回账户余额充足标记

### 消耗类指标

#### `adv_cost_ratio`
- `operatorType`: `metric`
- `operatorNameCn`: 账户消耗环比
- `groupName`: `cost`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[adv_cost_ratio](state_metric_operator_codes/adv_cost_ratio.md)

DF 口径摘要：
- 中文名：账户消耗环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_cost_ratio.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_cost_ratio.py:1)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- 依赖字段：
  - `adv_state[].adv_cost`
- 计算口径：
  - 仅支持 `adv_id`
  - 用 `calc_sequential_ratio(adv_cost, start_date, end_date)`

#### `ad_cost_ratio`
- `operatorType`: `metric`
- `operatorNameCn`: 计划消耗环比
- `groupName`: `cost`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[ad_cost_ratio](state_metric_operator_codes/ad_cost_ratio.md)

DF 口径摘要：
- 中文名：计划消耗环比
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_cost_ratio.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_cost_ratio.py:1)
- 外部输入字段：`state`, `id`, `start_date`, `end_date`
- 依赖字段：
  - `ad_state[].ad_cost`
- 计算口径：
  - 仅支持 `ad_id`
  - 用 `calc_sequential_ratio(ad_cost, start_date, end_date)`
  - 输出上周期、本周期和环比

### 违规类指标

#### `adv_violation_info`
- `operatorType`: `metric`
- `operatorNameCn`: 账号是否存在违规
- `groupName`: `risk`
- `inputParameter`: `{"params":[]}`，如后续增加业务 placeholder 参数，再按当前算子规则补充。
- 代码文档：[adv_violation_info](state_metric_operator_codes/adv_violation_info.md)

DF 口径摘要：
- 中文名：账号是否存在违规
- 代码位置：[/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_violation_info.py](/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_violation_info.py:1)
- 外部输入字段：`state`, `id`
- 依赖字段：
  - `ad_state[].related_adv_id`
  - `adv_state[].adv_id`
  - `adv_state[].adv_violation_info`
- 计算口径：
  - `ad_id` 口径先映射到账户 ID
  - 再返回账户违规标记

## 4. 维护建议

当前仓库里，指标元数据不是集中维护的，而是分散在两层：

1. `registry.py`
   - 维护 `metricCode -> 中文名 -> 处理函数`
2. 各个 handler 文件
   - 维护“依赖哪些 state 字段”和“怎么计算”

如果后续要做正式元数据维护，建议至少沉淀这几个字段：

- `metricCode`
- `metricName`
- `module`
- `supports_id_keys`
- `external_inputs`
- `state_dependencies`
- `world_dependencies`
- `formula_or_logic`
- `output_template_type`

这样可以把“注册信息”和“计算元数据”分开管理。

## 5. Tool 口径摘要

#### `customer_info_acquisition`
- `operatorType`: `tool`
- `toolNameCn`: 客户信息获取
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `customer_info_acquisition`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{}`
- 代码文档：[customer_info_acquisition](state_metric_operator_codes/customer_info_acquisition.md)

#### `get_industry_creative_tips`
- `operatorType`: `tool`
- `toolNameCn`: 行业创意建议
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `get_industry_creative_tips`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"advId": id_value}`
- 代码文档：[get_industry_creative_tips](state_metric_operator_codes/get_industry_creative_tips.md)

#### `send_insight_report`
- `operatorType`: `tool`
- `toolNameCn`: 发送洞察报告
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `send_insight_report`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"advId": id_value}`
- 代码文档：[send_insight_report](state_metric_operator_codes/send_insight_report.md)

#### `ad_plan_diagnosis_sec`
- `operatorType`: `tool`
- `toolNameCn`: 广告计划诊断
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `ad_plan_diagnosis_sec`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"objectId": id_value}`
- 代码文档：[ad_plan_diagnosis_sec](state_metric_operator_codes/ad_plan_diagnosis_sec.md)

#### `retrieve_id_type`
- `operatorType`: `tool`
- `toolNameCn`: 识别ID类型
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `retrieve_id_type`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"unknownTypeIDs": [id_value]}`
- 代码文档：[retrieve_id_type](state_metric_operator_codes/retrieve_id_type.md)

#### `review_exception_diagnosis_and_promption`
- `operatorType`: `tool`
- `toolNameCn`: 审核异常诊断与申诉
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `review_exception_diagnosis_and_promption`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"properties": {"objectId": {"type": id_value}, "advId": {}}}`
- 代码文档：[review_exception_diagnosis_and_promption](state_metric_operator_codes/review_exception_diagnosis_and_promption.md)

#### `get_douplus_order_detail`
- `operatorType`: `tool`
- `toolNameCn`: 订单详情查询
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `get_douplus_order_detail`
- `inputParameter`: `{"params":[]}`
- DF tool_input 构造：`{"order_id": id_value}`
- 代码文档：[get_douplus_order_detail](state_metric_operator_codes/get_douplus_order_detail.md)

## 6. 维护注意事项

1. 文档中的 DF 原始代码不能直接作为 `operatorCode` 入库，除非已经改写为当前算子要求的 `calculate(...)`。
2. 不要在 `operatorCode` 中 import Dataset Factory 包；公共数学和日期能力通过 `helpers` 注入。
3. 输出文案先维护在 `operatorCode` 中；字符串直接 return，不需要手动 `json.dumps`。
4. `summary_success_only=false` 是当前推荐默认值，失败项会保留 `error`。
5. 每批同步后必须用后端 CLI 拉取确认，再用 Data-Juicer 最小 YAML 联调。
