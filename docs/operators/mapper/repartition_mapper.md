# repartition_mapper

Repartition a Ray Dataset into a target number of blocks.

将 Ray Dataset 重新分区到指定 block 数。

Type 算子类型: **mapper**

Tags 标签: ray, cpu

## 🔧 Parameter Configuration 参数配置
| name 参数名 | type 类型 | default 默认值 | desc 说明 |
|--------|------|--------|------|
| `num_blocks` | <class 'int'> | `1` | Target number of Ray Dataset blocks. |
| `shuffle` | <class 'bool'> | `False` | Whether to shuffle records during repartition. |

## 🔗 related links 相关链接
- [source code 源代码](../../../data_juicer/ops/mapper/repartition_mapper.py)
- [unit test 单元测试](../../../tests/ops/mapper/test_repartition_mapper.py)
- [Return operator list 返回算子列表](../../Operators.md)
