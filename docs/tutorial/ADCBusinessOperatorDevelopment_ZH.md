# ADC 业务算子开发教程

本文说明如何在 `data_juicer/ops/mapper/ad_ai_data_center` 下创建一个 ADC 业务算子。这里的业务算子指带有 ADC 平台标签、可选使用 `ctx`、并能上报 operator/record 执行状态的 Data-Juicer mapper。

典型场景包括：

- 封装一段固定业务逻辑，例如标准数据集组装。
- 封装一个 ADC OpenAPI 调用，例如 LLM 推理、State 模板生成。
- 封装一个可信 Python 处理逻辑，但不希望用户在 YAML 中直接传 `python_code`。

如果只是让用户自定义脚本，优先使用 `python_script_mapper`。如果已有算子通过 YAML 编排能表达需求，优先组合已有算子。

## 1. 目录和命名

业务算子代码放在：

```text
data_juicer/ops/mapper/ad_ai_data_center/
```

测试放在：

```text
tests/ops/mapper/
```

命名建议：

| 项 | 规则 | 示例 |
| --- | --- | --- |
| 文件名 | 小写下划线，后缀 `_mapper.py` | `standard_dataset_assembler_mapper.py` |
| `OP_NAME` | 和 YAML 节点名一致 | `standard_dataset_assembler_mapper` |
| 类名 | PascalCase | `StandardDatasetAssemblerMapper` |
| 测试文件 | `test_<op_name>.py` | `test_standard_dataset_assembler_mapper.py` |

最小常量示例：

```python
OP_NAME = "standard_dataset_assembler_mapper"
OP_DISPLAY_NAME = "标准数据集组装"
CONFIG_PAGE_KEY = "standard_dataset_assembler_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
```

说明：

| 常量 | 用途 |
| --- | --- |
| `OP_NAME` | Data-Juicer 注册名，也是 YAML 中的节点名。 |
| `OP_DISPLAY_NAME` | 前端或后端展示用中文名。 |
| `CONFIG_PAGE_KEY` | 前端配置页或 builder 标识。 |
| `NEED_CTX` | 标记该算子需要平台上下文。这里是标记，不代表构造函数必须强校验。 |
| `OPERATOR_TAG` | ADC 后端同步算子元信息时识别业务算子。业务算子统一使用 `business_operator`。 |

## 2. 最小算子模板

业务 mapper 继承 `Mapper`，并用 `OPERATORS.register_module(OP_NAME)` 注册。

```python
from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.operator_execution_callback_utils import (
    NoOpOperatorExecutionCallbackClient,
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
    has_operator_execution_callback_ctx,
)

OP_NAME = "my_business_mapper"
OP_DISPLAY_NAME = "我的业务算子"
CONFIG_PAGE_KEY = "my_business_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"


@OPERATORS.register_module(OP_NAME)
class MyBusinessMapper(Mapper):
    def __init__(self, ctx: dict | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ctx = ctx
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            output_sample = self._process_sample(copy.deepcopy(sample))
        except Exception as exc:
            self._report_record_failure(input_sample, None, str(exc), started_at)
            raise

        self._report_record_success(input_sample, output_sample, started_at)
        return output_sample

    def _process_sample(self, sample):
        sample["result"] = "ok"
        return sample
```

如果算子只封装固定逻辑，不要暴露 `python_code`。固定逻辑建议写成普通 Python 函数并由算子调用，便于测试和 CR。

## 3. ctx 处理规范

`ctx` 通常由后端注入，用于：

- operator execution start/finalize/failed 回调。
- record success/failure 回调。
- ADC OpenAPI 调用时组装 base URL、header、space-id 等。

推荐规则：

- 构造函数允许 `ctx=None`。
- 如果业务逻辑本身不依赖外部接口，没有 `ctx` 时仍应正常处理样本。
- 如果只有回调需要 `ctx`，使用 `NoOpOperatorExecutionCallbackClient`。
- 只有真正要调用后端接口时，才在接口调用路径上校验 `ctx.apiBase`、`ctx.userAccount`、`ctx.spaceId` 等必需字段。

回调 client 获取方式：

```python
def _get_operator_execution_callback_client(self):
    if self._operator_execution_callback_client is None:
        if not has_operator_execution_callback_ctx(self.ctx):
            callback_client = NoOpOperatorExecutionCallbackClient()
        else:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
        callback_client.start(operator_config=self._operator_config())
        self._operator_execution_callback_client = callback_client
    return self._operator_execution_callback_client
```

`_operator_config()` 应返回可序列化的配置摘要，方便后端记录本次执行配置。不要放入超大对象、不可序列化对象或敏感数据。

## 4. Record 回调规范

ADC 业务算子应在每条样本处理后上报 Record。

成功路径：

```python
def _report_record_success(self, input_sample, output_sample, started_at):
    try:
        output_record_key = self._maybe_get_record_key(output_sample)
        callback_kwargs = {
            "record_key": output_record_key,
            "input_data": input_sample,
            "output_data": copy.deepcopy(output_sample),
            "started_at": started_at,
        }
        if output_record_key is None:
            callback_kwargs["fallback_record_key"] = self._get_record_key(input_sample)
        self._get_operator_execution_callback_client().report_record_success(**callback_kwargs)
    except Exception as exc:
        logger.warning("Failed to report record success callback: {}", exc)
```

失败路径：

```python
def _report_record_failure(self, input_sample, output_sample, error_message, started_at):
    try:
        record_key_sample = output_sample if output_sample is not None else input_sample
        self._get_operator_execution_callback_client().report_record_failure(
            record_key=self._get_record_key(record_key_sample),
            input_data=input_sample,
            output_data=copy.deepcopy(output_sample) if output_sample is not None else None,
            error_message=error_message,
            started_at=started_at,
        )
    except Exception as exc:
        logger.warning("Failed to report record failure callback: {}", exc)
```

Record key 规则必须和现有业务算子保持一致：

```python
@staticmethod
def _get_record_key(sample: dict[str, Any]):
    if not sample.get(RECORD_KEY_FIELD):
        raise ValueError(f"sample.{RECORD_KEY_FIELD} must be provided")
    return sample[RECORD_KEY_FIELD]


@staticmethod
def _maybe_get_record_key(sample: dict[str, Any] | None):
    if not isinstance(sample, dict):
        return None
    value = sample.get(RECORD_KEY_FIELD)
    return value if value not in (None, "") else None
```

行为说明：

| case | 表现 |
| --- | --- |
| 输出样本保留 `__adc_record_key` | 直接使用输出样本的 key 上报。 |
| 输出样本删除 `__adc_record_key`，输入样本有 key | 使用 `fallback_record_key`，回调正常。 |
| 输入和输出都没有 `__adc_record_key` | 业务结果不因回调失败中断，但日志中应出现 `sample.__adc_record_key must be provided`。 |
| 业务处理失败 | 尝试上报 record failure，然后继续抛出原业务异常。 |

`__adc_record_key` 通常由 `prepare_record_key_mapper` 在流程前置生成。新业务算子不应自行生成这个 key，除非它本身就是 record key 准备类算子。

## 5. 整条 sample 修改规范

多数 mapper 只新增或改写字段，直接返回 `sample` 即可。

如果算子语义是“重建整条样本”，例如标准数据集组装，需要注意本地 `NestedDataset` / HuggingFace Dataset 的 `map` 可能保留旧列。此时可以在 `run()` 后移除非标准列：

```python
def run(self, dataset, *, exporter=None, tracer=None):
    dataset = super().run(dataset, exporter=exporter, tracer=tracer)
    return self._remove_non_standard_columns(dataset)
```

只在能直接读取本地 `column_names` 的数据集上移除列：

```python
def _remove_non_standard_columns(self, dataset):
    columns = self._dataset_columns(dataset)
    if not columns:
        return dataset

    removable = [column for column in columns if column not in KEEP_KEYS]
    if not removable:
        return dataset

    remove_columns = getattr(dataset, "remove_columns", None)
    if callable(remove_columns):
        return remove_columns(removable)

    return dataset


@staticmethod
def _dataset_columns(dataset):
    column_names = getattr(dataset, "column_names", None)
    if column_names is not None:
        return list(column_names)
    return []
```

不要为了删列调用 Ray Dataset 的 `columns()`。它可能触发 schema 推断或预执行，影响上游执行节奏。

## 6. 默认脚本封装规范

如果算子封装一段固定 Python 脚本，推荐做法是：

1. 将固定逻辑维护为普通 Python 文件。
2. 算子直接 import 函数调用。
3. YAML 不暴露 `python_code`。
4. 测试直接覆盖算子的真实控制流。

标准数据集组装算子的做法：

```python
from docs.reference.standard_dataset_assembler import (
    KEEP_KEYS,
    process as assemble_standard_dataset,
)


def process_single(self, sample):
    started_at = current_time_millis()
    input_sample = copy.deepcopy(sample)
    try:
        output_sample = assemble_standard_dataset(copy.deepcopy(sample), {})
    except Exception as exc:
        self._report_record_failure(input_sample, None, str(exc), started_at)
        raise

    self._report_record_success(input_sample, output_sample, started_at)
    return output_sample
```

这里对外只需要 sample。传给默认脚本的 context 可以是空 dict；`ctx` 只用于平台回调，不参与业务逻辑。

如果默认脚本未来需要读取上下文，应先明确它读取的是 Data-Juicer runtime context 还是 ADC `ctx`，不要混用。

## 7. YAML 配置示例

最小配置：

```yaml
process:
  - standard_dataset_assembler_mapper:
      ctx:
        userAccount: "zhangsan"
        apiBase: "https://ai-data-center.bytedance.net/api"
        spaceId: 1
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "task_1"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "standard_dataset_assembler_mapper"
        operatorType: "Mapper"
```

如果没有 `ctx`，标准数据集组装这类纯本地逻辑仍可执行，但不会产生真实回调。

## 8. 测试清单

新增 ADC 业务算子至少覆盖下面几类测试：

| 测试 | 目的 |
| --- | --- |
| `process_single` 或 `op.run(Dataset.from_list(...))` 成功路径 | 验证真实样本处理结果。 |
| success record callback | 验证 `record_key` / `fallback_record_key`、input/output、started_at。 |
| failure record callback | 验证业务异常时会上报失败记录，并继续抛出原异常。 |
| missing ctx | 验证没有 ctx 时业务逻辑不被回调阻塞。 |
| missing `__adc_record_key` | 验证缺 key 时业务结果不被回调阻塞，warning 原因清晰。 |
| YAML load_ops | 验证无需 `ad_ai_data_center.` 前缀即可加载算子。 |
| 常量断言 | 验证 `OP_NAME`、`OP_DISPLAY_NAME`、`CONFIG_PAGE_KEY`、`NEED_CTX`。 |

示例断言：

```python
self.assertEqual(OP_NAME, "standard_dataset_assembler_mapper")
self.assertEqual(OP_DISPLAY_NAME, "标准数据集组装")
self.assertEqual(CONFIG_PAGE_KEY, "standard_dataset_assembler_builder")
self.assertEqual(NEED_CTX, True)
```

回调测试建议 patch 当前算子模块里的 `OperatorExecutionCallbackClient`，不要 patch 工具模块里的类，否则算子已经导入的引用不会被替换。

```python
@patch(
    "data_juicer.ops.mapper.ad_ai_data_center.standard_dataset_assembler_mapper."
    "OperatorExecutionCallbackClient"
)
def test_record_callback(self, mock_callback_cls):
    ...
```

## 9. CR 检查清单

提交前按下面顺序自查：

1. `OP_NAME` 和 YAML 节点名一致。
2. `OP_DISPLAY_NAME` 是产品确认过的中文名。
3. `OPERATOR_TAG = "business_operator"`。
4. `ctx` 不被无意义强校验；没有 ctx 时，本地逻辑可以继续执行。
5. 需要调用 ADC OpenAPI 的路径正确校验 `apiBase`、`userAccount`、`spaceId`。
6. success/failure record callback 都实现。
7. `__adc_record_key` 缺失行为和 `python_script_mapper` 保持一致。
8. 回调异常只记录 warning，不阻断业务结果。
9. 如果算子重建整条 sample，本地 Dataset 旧列不会泄漏到最终输出。
10. 没有调用会触发 Ray schema 预执行的 API。
11. 测试覆盖真实 `load_ops` 控制流，而不是只测构造函数。
12. 运行聚焦测试、`py_compile` 和 `git diff --check`。

## 10. 标准数据集组装参考

当前可参考的完整实现：

- 算子：[standard_dataset_assembler_mapper.py](../../data_juicer/ops/mapper/ad_ai_data_center/standard_dataset_assembler_mapper.py)
- 默认脚本：[standard_dataset_assembler.py](../reference/standard_dataset_assembler.py)
- 测试：[test_standard_dataset_assembler_mapper.py](../../tests/ops/mapper/test_standard_dataset_assembler_mapper.py)

这个算子的关键特征：

- 对外没有额外业务参数。
- `ctx` 只用于平台回调。
- 默认脚本直接修改并返回整条 sample。
- 输出只保留标准数据集字段。
- Record 回调行为与 `python_script_mapper` 保持一致。
