# Data-Juicer 算子类型对比

Data-Juicer 的算子不是按数据模态划分，而是按“对样本集合做什么事情”划分。文本、图片、音频、视频、多模态数据都可以落到这些算子类型里。

## 总览

| 类型 | 核心职责 | 判断或处理对象 | 是否通常会删除样本 | 典型产物 |
| --- | --- | --- | --- | --- |
| `Mapper` | 改写样本或新增字段 | 单条样本或一批样本 | 通常不会 | 新字段、清洗后的文本、生成的图片/视频/标注 |
| `Filter` | 判断样本是否保留 | 单条样本 | 会 | 被过滤后的数据集 |
| `Deduplicator` | 删除重复或近似重复样本 | 样本之间的关系 | 会 | 去重后的数据集、hash 字段 |
| `Selector` | 从数据集中选择一部分样本 | 样本集合 | 会 | 抽样后的数据集 |
| `Grouper` | 将样本组织成组 | 样本集合 | 视实现而定 | 分组后的数据结构 |
| `Aggregator` | 汇总多个字段或多条信息 | 样本、字段、分组结果 | 通常不会 | 聚合字段、标签、实体或统计结果 |
| `Pipeline` | 封装多阶段处理流程 | 数据集或 Ray 数据集 | 视内部流程而定 | 多阶段处理后的数据集 |

## Mapper

`Mapper` 用于改变样本内容。它通常读取当前样本的一个或多个字段，然后写回原字段或新增字段。

适合写成 `Mapper` 的场景：

- 清洗文本：去 HTML、去邮箱、去链接、规范空白字符。
- 转换字段：把一种 schema 转成另一种 schema。
- 生成内容：调用模型生成 caption、QA、标签、摘要。
- 处理多媒体：裁剪图片、抽取视频帧、音频增强。

典型例子：

- `whitespace_normalization_mapper`
- `clean_html_mapper`
- `image_captioning_mapper`
- `video_extract_frames_mapper`

选择标准：如果你的逻辑是“输入一条样本，输出一条被改写后的样本”，优先考虑 `Mapper`。

## Filter

`Filter` 用于判断单条样本是否合格。它的核心输出是布尔值：`True` 表示保留，`False` 表示删除。

适合写成 `Filter` 的场景：

- 文本长度是否在范围内。
- 语言识别分数是否达标。
- 图片尺寸、比例、清晰度是否合规。
- 视频时长、分辨率、运动分数是否达标。
- 某个字段是否存在、是否为空、是否满足数值条件。

典型例子：

- `text_length_filter`
- `language_id_score_filter`
- `image_size_filter`
- `video_duration_filter`

选择标准：如果规则只依赖当前样本自己的字段或统计值，写 `Filter`。

## Deduplicator

`Deduplicator` 用于删除重复或近似重复样本。它和 `Filter` 的关键区别是：`Deduplicator` 关心样本之间的关系，而不是只看当前样本。

适合写成 `Deduplicator` 的场景：

- 文档完全重复去重。
- 文档近似重复去重，例如基于 `simhash` 或 `minhash`。
- 图片重复或相似图片去重。
- 视频重复或相似视频去重。

典型例子：

- `document_deduplicator`
- `document_minhash_deduplicator`
- `document_simhash_deduplicator`
- `image_deduplicator`
- `video_deduplicator`

选择标准：如果删除样本的依据是“它和其他样本重复或相似”，写 `Deduplicator`。

### Filter 和 Deduplicator 的边界

| 问题 | 应选类型 |
| --- | --- |
| 这条样本是否太短？ | `Filter` |
| 这条样本语言分数是否太低？ | `Filter` |
| 这张图片尺寸是否太小？ | `Filter` |
| 这条样本是否和另一条样本内容相同？ | `Deduplicator` |
| 这篇文档是否和其他文档近似重复？ | `Deduplicator` |
| 这段视频是否和其他视频相似？ | `Deduplicator` |

例子：

```text
A: "今天天气很好"
B: "今天天气很好"
C: "短文本"
```

`Filter` 可能因为 C 太短而删除 C；`Deduplicator` 可能因为 A 和 B 重复而只保留其中一条。

## Selector

`Selector` 用于从数据集中选择一部分样本。它通常不是判断“样本是否合格”，而是执行一种抽样或选择策略。

适合写成 `Selector` 的场景：

- 随机抽样。
- 按字段频次抽样。
- 按某个分数取 TopK。
- 按标签或字段条件选择子集。

典型例子：

- `random_selector`
- `topk_specified_field_selector`
- `frequency_specified_field_selector`
- `tags_specified_field_selector`

选择标准：如果目标是“从整体数据中挑出一部分”，而不是定义质量规则，写 `Selector`。

## Grouper

`Grouper` 用于把样本按某种规则组织成组。它关注的是样本集合的组织方式。

适合写成 `Grouper` 的场景：

- 按字段值分组。
- 按 key/value 关系组织样本。
- 将扁平样本变成分组结构，或反向展开。

典型例子：

- `key_value_grouper`
- `naive_grouper`
- `naive_reverse_grouper`

选择标准：如果核心操作是“把样本组织成组或从组中展开”，写 `Grouper`。

## Aggregator

`Aggregator` 用于聚合信息。它通常把多个字段、多个标签、多个实体或分组结果汇总成更高层的结果。

适合写成 `Aggregator` 的场景：

- 汇总样本中的实体属性。
- 从多个候选实体中选出最相关实体。
- 聚合 meta 标签。
- 对嵌套结构进行汇总。

典型例子：

- `entity_attribute_aggregator`
- `meta_tags_aggregator`
- `most_relevant_entities_aggregator`
- `nested_aggregator`

选择标准：如果你的逻辑是“把已有信息汇总成一个更高层结果”，写 `Aggregator`。

## Pipeline

`Pipeline` 用于封装一组更复杂的多阶段处理逻辑。它适合把多个步骤作为一个整体暴露给用户。

适合写成 `Pipeline` 的场景：

- LLM/VLM 推理链路。
- Ray + vLLM 批处理流程。
- 多个内部处理阶段需要共享状态或统一调度。
- 单个 `Mapper` 或 `Filter` 已经不足以表达完整流程。

典型例子：

- `ray_vllm_pipeline`
- `llm_inference_with_ray_vllm_pipeline`
- `vlm_inference_with_ray_vllm_pipeline`

选择标准：如果这不是一个单点转换、过滤或去重，而是一条小型处理流水线，写 `Pipeline`。

## 开发时的选择顺序

可以按下面的问题快速判断：

1. 是否只是改写字段或新增字段？是的话写 `Mapper`。
2. 是否判断当前样本是否合格？是的话写 `Filter`。
3. 是否判断样本之间是否重复或相似？是的话写 `Deduplicator`。
4. 是否从整体数据中抽出一部分？是的话写 `Selector`。
5. 是否把样本组织成组或展开分组？是的话写 `Grouper`。
6. 是否把已有信息汇总成更高层结果？是的话写 `Aggregator`。
7. 是否是一条包含多个阶段的处理链路？是的话写 `Pipeline`。

如果仍然不确定，优先从 `Mapper` 或 `Filter` 开始。它们的接口最稳定、测试成本最低，也最容易接入 YAML 配方。
