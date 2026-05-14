# 算子开发 Quick Start

这份文档用于快速写出第一个 Data-Juicer 算子，并用最小测试和 YAML 配置跑通真实处理链路。完整规范可继续参考 [DeveloperGuide_ZH.md](DeveloperGuide_ZH.md) 和 [Operators.md](Operators.md)。

## 1. 先判断算子类型

新增算子前，先确认它属于哪一类：

| 类型 | 作用 | 适合场景 |
| --- | --- | --- |
| `Mapper` | 修改样本或新增字段 | 文本清洗、字段转换、生成标注、图片/视频处理 |
| `Filter` | 判断样本是否保留 | 长度过滤、质量分过滤、规则过滤 |
| `Deduplicator` | 去重 | 文档、图片、视频或近似去重 |
| `Selector` | 选择样本子集 | 随机抽样、TopK、按字段抽样 |
| `Grouper` | 分组 | 按字段或规则组织样本 |
| `Aggregator` | 聚合 | 将多个字段、标签或实体汇总 |
| `Pipeline` | 组合一组处理逻辑 | LLM/VLM 推理、多阶段处理 |

第一个算子建议从 `Mapper` 或 `Filter` 开始，避免一开始就处理 Ray 去重、GPU runtime、外部模型等复杂路径。

## 2. Filter 和 Deduplicator 怎么选

`Filter` 和 `Deduplicator` 都可能删除样本，但判断依据不同：

| 类型 | 判断对象 | 输出含义 | 典型例子 |
| --- | --- | --- | --- |
| `Filter` | 单条样本 | 这条样本是否合格 | 长度过滤、语言过滤、质量分过滤 |
| `Deduplicator` | 样本集合中的相互关系 | 这条样本是否是重复项 | 文档去重、图片去重、视频去重 |

`Filter` 是单样本判断。它只看当前样本自己的内容、字段或统计值，然后返回 `True` 或 `False`。例如文本长度是否达标、语言是否为中文、图片尺寸是否合规。

`Deduplicator` 是样本之间的比较。它需要判断多条样本之间是否重复或近似重复，通常会先计算 `hash`、`simhash`、`minhash`、`imagehash`、`videohash` 等，再找出重复组并删除重复项。

例如：

```text
A: "今天天气很好"
B: "今天天气很好"
C: "这是一段特别短的文本"
```

如果写 `Filter`，C 可能因为长度太短被删除，A 和 B 是否相同不是它主要关心的问题。

如果写 `Deduplicator`，A 和 B 因为内容重复会只保留一条；C 只要不和其他样本重复，就可能被保留。

选择标准：

- 规则只依赖当前样本：写 `Filter`。
- 规则依赖样本之间的重复或相似关系：写 `Deduplicator`。

## 3. 选择放置位置

内置算子按类型放在 `data_juicer/ops/` 下：

```text
data_juicer/ops/
  mapper/
  filter/
  deduplicator/
  selector/
  grouper/
  aggregator/
  pipeline/
```

例如：

- 新增文本清洗算子：`data_juicer/ops/mapper/my_clean_mapper.py`
- 新增过滤算子：`data_juicer/ops/filter/my_rule_filter.py`

如果只是本地试验，也可以把算子放在仓库外部，通过 `--custom-operator-paths` 或 YAML 中的 `custom_operator_paths` 加载。

```yaml
custom_operator_paths:
  - /path/to/my_op.py
```

## 4. 最小 Mapper 模板

`Mapper` 用于改写样本。下面例子把文本首尾空白去掉，并写回 `text_key` 对应字段。

```python
from data_juicer.ops.base_op import OPERATORS, Mapper


@OPERATORS.register_module("strip_text_mapper")
class StripTextMapper(Mapper):
    """Strip leading and trailing whitespace from text samples."""

    _batched_op = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_batched(self, samples):
        for idx, text in enumerate(samples[self.text_key]):
            samples[self.text_key][idx] = text.strip()
        return samples
```

YAML 中这样使用：

```yaml
process:
  - strip_text_mapper: {}
```

## 5. 最小 Filter 模板

`Filter` 返回布尔值，`True` 表示保留样本，`False` 表示过滤样本。

```python
from data_juicer.ops.base_op import OPERATORS, Filter


@OPERATORS.register_module("min_text_length_filter")
class MinTextLengthFilter(Filter):
    """Keep samples whose text length is greater than or equal to min_len."""

    _batched_op = True

    def __init__(self, min_len: int = 10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_len = min_len

    def compute_stats_batched(self, samples):
        return samples

    def process_batched(self, samples):
        return [len(text) >= self.min_len for text in samples[self.text_key]]
```

YAML 中这样使用：

```yaml
process:
  - min_text_length_filter:
      min_len: 10
```

## 6. 注册和导出

算子能被 YAML 识别的关键是装饰器：

```python
@OPERATORS.register_module("min_text_length_filter")
```

如果算子作为内置算子提交到仓库，还需要把类名加入对应目录的 `__init__.py` 的 `__all__`，便于 Python API 和文档工具发现。例如 Filter 算子加入：

```python
__all__ = [
    # ...
    "MinTextLengthFilter",
]
```

当前仓库的算子模块支持按需懒加载，通常不需要在 `__init__.py` 里手动 import 新算子文件。

## 7. 写最小单测

行为变更必须有单测。Filter 可以参考下面的最小测试思路：

```python
import unittest

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.filter.min_text_length_filter import MinTextLengthFilter


class MinTextLengthFilterTest(unittest.TestCase):

    def test_filter_by_min_length(self):
        dataset = Dataset.from_list([
            {"text": "short"},
            {"text": "long enough"},
        ])
        op = MinTextLengthFilter(min_len=10)

        result = dataset.process([op], open_monitor=False)

        self.assertEqual(result.select_columns(["text"]).to_list(), [
            {"text": "long enough"},
        ])


if __name__ == "__main__":
    unittest.main()
```

建议测试真实路径 `dataset.process([op])`，不要只测试构造函数或私有 helper。

## 8. 用 YAML 跑通

准备一个最小配置：

```yaml
project_name: demo-custom-op
dataset_path: ./demos/data/demo-dataset.jsonl
np: 1
export_path: ./outputs/demo-custom-op/result.jsonl

process:
  - min_text_length_filter:
      min_len: 10
```

运行：

```bash
./.venv/bin/python tools/process_data.py --config path/to/config.yaml
```

如果只是检查语法：

```bash
python3 -m py_compile data_juicer/ops/filter/min_text_length_filter.py
```

语法检查不能替代单测，至少还要跑对应测试：

```bash
./.venv/bin/python -m unittest tests.ops.filter.test_min_text_length_filter
```

## 9. 依赖和性能注意事项

- 不要在模块顶层导入重依赖，例如 `torch`、`cv2`、模型 SDK 或内部服务 SDK。
- 优先在 `__init__` 或实际调用方法中按需加载重依赖。
- 能批处理时使用 `_batched_op = True`，并实现 `process_batched` / `compute_stats_batched`。
- 需要 GPU 时再声明 `_accelerator = "cuda"`，并确认方法签名兼容 `rank` 参数。
- 如果算子生成额外文件，使用已有的 produced data 机制或显式配置输出目录，避免覆盖输入数据。
- 如果要支持 Ray 运行，避免不可序列化对象提前初始化，并为外部依赖准备 runtime env。

## 10. 开发检查清单

- [ ] 已确认没有重复的内置算子。
- [ ] 文件放在正确的 `ops/<type>/` 目录。
- [ ] 使用 `@OPERATORS.register_module("snake_case_op_name")` 注册。
- [ ] 参数写在 `__init__`，并提供清晰默认值。
- [ ] docstring 说明用途、输入、输出和主要参数。
- [ ] 内置算子已更新对应目录 `__init__.py` 的 `__all__`。
- [ ] 已添加最小单测，覆盖真实处理路径。
- [ ] 已用 YAML 或 Python API 做过一次端到端验证。
- [ ] 新依赖已按需加载，避免影响无关算子启动。
