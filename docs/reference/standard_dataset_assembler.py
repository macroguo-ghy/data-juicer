import hashlib
import json
import re
import uuid as uuid_lib

SECTION_FIELD_PATHS = {
    "meta_info": [
        ("meta_info/unique_id", ["unique_id"]),
        ("meta_info/scenario/scenario_id", ["scenario", "scenario_id"]),
        ("meta_info/scenario/task_type", ["scenario", "task_type"]),
        ("meta_info/scenario/business_line", ["scenario", "business_line"]),
        ("meta_info/scenario/query_type", ["scenario", "query_type"]),
        ("meta_info/scenario/event_type", ["scenario", "event_type"]),
    ],
    "input": [
        ("input/user_query", ["user_query"]),
        ("input/response", ["response"]),
        ("input/trajectory", ["trajectory"]),
        ("input/task", ["task"]),
        ("input/supplementary_info", ["supplementary_info"]),
    ],
    "context": [
        ("context/memory/chat_history", ["memory", "chat_history"]),
        ("context/memory/user_profile", ["memory", "user_profile"]),
        ("context/memory/extracted_facts", ["memory", "extracted_facts"]),
        ("context/env_state", ["env_state"]),
    ],
    "reference": [
        ("reference/routing", ["routing"]),
        ("reference/function_call", ["function_call"]),
        ("reference/content", ["content"]),
        ("reference/rubric_answers", ["rubric_answers"]),
        ("reference/ground_truth", ["ground_truth"]),
        ("reference/alternative_ground_truth", ["alternative_ground_truth"]),
        ("reference/optional_answer", ["optional_answer"]),
    ],
    "extra": [
        ("extra/chat_id", ["chat_id"]),
        ("extra/message_id", ["message_id"]),
    ],
}

KEEP_KEYS = {
    "meta_info",
    "input",
    "context",
    "rubrics",
    "reference",
    "extra",
}

RUBRIC_FIELD_PATTERN = re.compile(r"^rubrics/[^/]+/(meta|dimensions|dimensions_json)$")


def process(sample, context):
    """
    功能描述：将斜杠列名聚合为标准结构，兼容两种 rubrics 来源，生成 fingerprint 和 unique_id，并仅保留标准输出字段。
    入参：
      - sample: 当前完整样本 dict，可读取、修改或新增字段。
      - context: 平台上下文，用于 unique_id 兜底 seed。
    出参：
      - dict: 返回仅包含 meta_info、input、context、rubrics、reference、extra 的样本。
    依赖说明：
      - 仅依赖 Python 标准库 hashlib、json、re、uuid。
      - fingerprint 基于最终样本生成，但排除 meta_info.unique_id 和 meta_info.fingerprint。
      - unique_id 使用 uuid.uuid5(uuid.NAMESPACE_URL, seed_name)。
    """
    source_keys_to_delete = []

    for root_key, field_paths in SECTION_FIELD_PATHS.items():
        section, used_keys = build_section(sample, field_paths)
        source_keys_to_delete.extend(used_keys)
        if section:
            sample[root_key] = section

    if has_rubric_source_fields(sample):
        build_rubrics(sample)
        source_keys_to_delete.extend(collect_rubric_source_keys(sample))
    else:
        normalize_rubrics(sample)

    for key in source_keys_to_delete:
        sample.pop(key, None)

    for key in list(sample.keys()):
        if key not in KEEP_KEYS:
            sample.pop(key, None)
    # 统一序列化 平台方便解析
    if "rubrics" in sample and not isinstance(sample["rubrics"], str):
        sample["rubrics"] = json.dumps(sample["rubrics"], ensure_ascii=False)

    ensure_meta_info(sample)
    sample["meta_info"]["fingerprint"] = generate_fingerprint(sample)
    sample["meta_info"]["unique_id"] = generate_unique_id(sample, context)

    return sample


def build_section(sample, field_paths):
    """
    功能描述：按配置将扁平斜杠字段组装为一个嵌套对象。
    入参：
      - sample: 当前完整样本 dict。
      - field_paths: 字段映射列表，元素格式为 (source_key, target_path)。
    出参：
      - tuple: (section, used_keys)，section 为组装后的对象，used_keys 为已消费源字段。
    依赖说明：
      - 依赖 is_non_empty_value、transform_section_value、set_nested_value。
    """
    section = {}
    used_keys = []

    for source_key, target_path in field_paths:
        if source_key not in sample:
            continue

        used_keys.append(source_key)
        value = sample.get(source_key)

        if not is_non_empty_value(value):
            continue

        value = transform_section_value(source_key, value)
        set_nested_value(section, target_path, value)

    return section, used_keys


def transform_section_value(source_key, value):
    """
    功能描述：按源字段对字段值做特殊转换。
    入参：
      - source_key: 原始斜杠列名。
      - value: 原始字段值。
    出参：
      - object: 转换后的字段值。
    依赖说明：
      - 仅依赖 json 标准库。
    """
    if source_key == "meta_info/unique_id":
        return str(value)

    if source_key == "context/memory/user_profile":
        return json.dumps(
            {"profiles": {"user_profile": str(value)}},
            ensure_ascii=False,
        )

    return value


def set_nested_value(target, path, value):
    """
    功能描述：向 dict 中按路径写入嵌套字段。
    入参：
      - target: 待写入的 dict。
      - path: 目标路径列表。
      - value: 待写入值。
    出参：
      - None: 原地修改 target。
    依赖说明：
      - 无外部依赖。
    """
    current = target

    for key in path[:-1]:
        if key not in current or not isinstance(current.get(key), dict):
            current[key] = {}
        current = current[key]

    current[path[-1]] = value


def is_non_empty_value(value):
    """
    功能描述：判断字段是否有有效值，空字符串、None、空列表、空字典均视为无值。
    入参：
      - value: 任意字段值。
    出参：
      - bool: 有值返回 True，否则返回 False。
    依赖说明：
      - 无外部依赖。
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def has_rubric_source_fields(sample):
    """
    功能描述：判断当前样本是否使用 rubrics/rubric*/meta、dimensions、dimensions_json 形式提供 rubrics。
    入参：
      - sample: 当前完整样本 dict。
    出参：
      - bool: 存在 rubrics 源字段返回 True，否则返回 False。
    依赖说明：
      - 依赖 re 标准库中的 RUBRIC_FIELD_PATTERN。
    """
    for key in sample.keys():
        if RUBRIC_FIELD_PATTERN.match(key):
            return True

    nested_rubrics = sample.get("rubrics")
    if not isinstance(nested_rubrics, dict):
        return False

    if "meta" in nested_rubrics and "config" in nested_rubrics:
        return False

    for rubric_value in nested_rubrics.values():
        if isinstance(rubric_value, dict) and any(
                key in rubric_value for key in ["meta", "dimensions", "dimensions_json"]
        ):
            return True

    return False


def normalize_rubrics(sample):
    """
    功能描述：保留已组装好的 rubrics，并确保其为评审代码可读取的数组结构。
    入参：
      - sample: 当前完整样本 dict，rubrics 可为 JSON 字符串、标准数组或单个标准对象。
    出参：
      - None: 原地修改 sample["rubrics"]。
    依赖说明：
      - 仅依赖 json 标准库。
    """
    if "rubrics" not in sample:
        return

    value = sample.get("rubrics")
    if not is_non_empty_value(value):
        return

    rubrics = json.loads(value) if isinstance(value, str) else value

    if isinstance(rubrics, list):
        sample["rubrics"] = rubrics
        return

    if isinstance(rubrics, dict) and "meta" in rubrics and "config" in rubrics:
        sample["rubrics"] = [rubrics]
        return


def build_rubrics(sample):
    """
    功能描述：将 rubrics/rubric*/meta、dimensions、dimensions_json 组装为标准 rubrics 数组。
    入参：
      - sample: 当前完整样本 dict。
    出参：
      - None: 原地覆盖 sample["rubrics"]。
    依赖说明：
      - 依赖 collect_rubrics、parse_meta、build_dimensions、build_dimensions_from_json。
    """
    raw_rubrics = collect_rubrics(sample)
    standard_rubrics = []

    for rubric_key in sorted(raw_rubrics.keys()):
        raw_rubric = raw_rubrics.get(rubric_key, {})
        raw_meta = raw_rubric.get("meta", "")
        raw_dimensions = raw_rubric.get("dimensions", "")
        raw_dimensions_json = raw_rubric.get("dimensions_json", "")

        if (
                not is_non_empty_value(raw_meta)
                and not is_non_empty_value(raw_dimensions)
                and not is_non_empty_value(raw_dimensions_json)
        ):
            continue

        meta = parse_meta(raw_meta)
        en_name_prefix = meta.get("en_name", "")

        if is_non_empty_value(raw_dimensions_json):
            dimensions = build_dimensions_from_json(raw_dimensions_json)
        else:
            if is_non_empty_value(raw_dimensions) and not en_name_prefix:
                raise ValueError("{} 的 meta 缺少 en_name，无法生成 dimensions.en_name".format(rubric_key))
            dimensions = build_dimensions(raw_dimensions, en_name_prefix)

        standard_rubrics.append({
            "meta": meta,
            "config": {"dimensions": dimensions},
        })

    sample["rubrics"] = standard_rubrics


def collect_rubrics(sample):
    """
    功能描述：从 sample 中收集 rubrics 数据，支持 meta、dimensions、dimensions_json 三类字段。
    入参：
      - sample: 当前完整样本 dict。
    出参：
      - dict: 形如 {"rubric1": {"meta": "...", "dimensions": "...", "dimensions_json": "..."}} 的中间结构。
    依赖说明：
      - 依赖 re 标准库。
    """
    rubrics = {}

    nested_rubrics = sample.get("rubrics")
    if isinstance(nested_rubrics, dict):
        for rubric_key, rubric_value in nested_rubrics.items():
            if isinstance(rubric_value, dict):
                rubrics[rubric_key] = {
                    "meta": rubric_value.get("meta", ""),
                    "dimensions": rubric_value.get("dimensions", ""),
                    "dimensions_json": rubric_value.get("dimensions_json", ""),
                }

    field_pattern = re.compile(r"^rubrics/([^/]+)/(meta|dimensions|dimensions_json)$")
    for key, value in sample.items():
        match = field_pattern.match(key)
        if not match:
            continue

        rubric_key = match.group(1)
        field_name = match.group(2)

        if rubric_key not in rubrics:
            rubrics[rubric_key] = {}

        rubrics[rubric_key][field_name] = value

    return rubrics


def collect_rubric_source_keys(sample):
    """
    功能描述：收集 rubrics 相关斜杠源字段，用于最终删除原始列。
    入参：
      - sample: 当前完整样本 dict。
    出参：
      - list[str]: 需要删除的 rubrics 源字段列表。
    依赖说明：
      - 依赖 re 标准库。
    """
    keys = []

    for key in sample.keys():
        if RUBRIC_FIELD_PATTERN.match(key):
            keys.append(key)

    return keys


def parse_meta(raw_meta):
    """
    功能描述：解析标准 JSON 格式的 rubric.meta；兼容 dict 和旧版多行 key-value 文本。
    入参：
      - raw_meta: meta 原始值，优先为 JSON 字符串或 dict。
    出参：
      - dict: 标准 meta 对象。
    依赖说明：
      - 依赖 json、re 标准库。
    """
    if isinstance(raw_meta, dict):
        return raw_meta

    text = str(raw_meta).strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    meta = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r"[:：]\s*", line, maxsplit=1)
        if len(parts) == 2:
            meta[parts[0].strip()] = parts[1].strip()

    return {
        "id": meta.get("id", ""),
        "name": meta.get("name", ""),
        "en_name": meta.get("en_name", ""),
        "version": meta.get("version", ""),
        "description": meta.get("description", ""),
        "rubric_type": meta.get("rubric_type", ""),
    }


def build_dimensions_from_json(raw_value):
    """
    功能描述：将 dimensions_json 解析为标准 dimensions 数组。
    入参：
      - raw_value: JSON 字符串、list 或 dict；支持标准 dimensions 数组、{"dimensions": [...]}、{"config": {"dimensions": [...]}}。
    出参：
      - list[dict]: 标准 dimensions 数组。
    依赖说明：
      - 仅依赖 json 标准库。
    """
    parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value

    if isinstance(parsed, dict):
        if isinstance(parsed.get("dimensions"), list):
            parsed = parsed.get("dimensions")
        elif isinstance(parsed.get("config"), dict) and isinstance(parsed["config"].get("dimensions"), list):
            parsed = parsed["config"]["dimensions"]
        else:
            raise ValueError("dimensions_json 对象必须包含 dimensions 或 config.dimensions")

    if not isinstance(parsed, list) or not parsed:
        raise ValueError("dimensions_json 必须是非空数组")

    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError("dimensions_json[{}] 必须是标准 dimension 对象".format(index))

    return parsed


def build_dimensions(raw_text, en_name_prefix):
    """
    功能描述：将旧版 dimensions 多行文本转换为标准 dimensions 数组。
    入参：
      - raw_text: 多行文本，每行格式为“{维度名称}-{分值}分-{权重}-{定义}”。
      - en_name_prefix: 从 meta.en_name 读取到的 en_name 前缀。
    出参：
      - list[dict]: 标准 dimensions 数组。
    依赖说明：
      - 依赖 parse_dimension_line。
    """
    grouped = {}

    for line in str(raw_text).splitlines():
        line = line.strip()
        if not line:
            continue

        parsed = parse_dimension_line(line)
        if parsed is None:
            raise ValueError("dimensions 行格式错误，期望：{维度名称}-{分值}分-{权重}-{定义}，实际：{}".format(line))

        unique_key = "{}||{}".format(parsed["name"], parsed["definition_key"])
        if unique_key not in grouped:
            grouped[unique_key] = {
                "name": parsed["name"],
                "weight": parsed["weight"],
                "description": parsed["definition_key"],
                "score_definitions": {},
            }

        grouped[unique_key]["weight"] = parsed["weight"]
        grouped[unique_key]["score_definitions"][parsed["score"]] = parsed["definition"]

    dimensions = []

    for index, item in enumerate(grouped.values()):
        description = item["description"]
        score_definitions = item["score_definitions"]

        dimensions.append({
            "name": item["name"],
            "weight": item["weight"],
            "en_name": "{}_{}".format(en_name_prefix, index),
            "description": description,
            "scoring_type": "binary",
            "score_definitions": {
                "0": score_definitions.get("0", "{}否为0".format(description)),
                "1": score_definitions.get("1", "{}是为1".format(description)),
            },
        })

    return dimensions


def parse_dimension_line(line):
    """
    功能描述：解析单行 dimensions 文本。
    入参：
      - line: 单行文本，格式为“{维度名称}-{分值}分-{权重}-{定义}”。
    出参：
      - dict 或 None: 解析成功返回 name、score、weight、definition、definition_key。
    依赖说明：
      - 依赖 re 标准库。
    """
    match = re.match(r"^(?P<name>.+)-(?P<score>[01])分-(?P<weight>[123])-(?P<definition>.+)$", line)
    if not match:
        return None

    definition = match.group("definition").strip()

    return {
        "name": match.group("name").strip(),
        "score": match.group("score"),
        "weight": int(match.group("weight")),
        "definition": definition,
        "definition_key": normalize_definition(definition),
    }


def normalize_definition(definition):
    """
    功能描述：从“xxx否为0 / xxx是为1”中提取公共 description。
    入参：
      - definition: 单条分值定义文本。
    出参：
      - str: 去掉分值后缀后的 description。
    依赖说明：
      - 无外部依赖。
    """
    for suffix in ["否为0", "是为1"]:
        if definition.endswith(suffix):
            return definition[:-len(suffix)]

    return definition


def ensure_meta_info(sample):
    """
    功能描述：确保 sample["meta_info"] 存在且为 dict。
    入参：
      - sample: 当前样本 dict。
    出参：
      - None: 原地修改 sample。
    依赖说明：
      - 无外部依赖。
    """
    if not isinstance(sample.get("meta_info"), dict):
        sample["meta_info"] = {}


def generate_fingerprint(sample):
    """
    功能描述：基于最终组装样本生成稳定 fingerprint，排除 meta_info.unique_id 和 meta_info.fingerprint。
    入参：
      - sample: 已完成字段聚合、rubrics 规范化、白名单清理的样本 dict。
    出参：
      - str: sha256 十六进制 fingerprint。
    依赖说明：
      - 依赖 hashlib、json 标准库。
    """
    payload = deep_copy(sample)

    meta_info = payload.get("meta_info")
    if isinstance(meta_info, dict):
        meta_info.pop("unique_id", None)
        meta_info.pop("fingerprint", None)

    canonical_payload = json.dumps(
        normalize_for_fingerprint(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def generate_unique_id(sample, context):
    """
    功能描述：按 smartsales_data_gen normalization.py 的 UuidProcessor 默认逻辑生成 unique_id。
    入参：
      - sample: 已写入 meta_info.fingerprint 的样本 dict。
      - context: 平台上下文，用于最终兜底 dataset + line_number。
    出参：
      - str: UUID5 字符串。
    依赖说明：
      - 依赖 uuid、json 标准库。
    """
    existing_value = get_by_path(sample, "meta_info.unique_id")

    if is_uuid_value(existing_value):
        return existing_value.strip()

    seed_payload = build_default_uuid_seed_payload(sample, context, existing_value)
    seed_name = json.dumps(
        seed_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return str(uuid_lib.uuid5(uuid_lib.NAMESPACE_URL, seed_name))


def build_default_uuid_seed_payload(sample, context, existing_value):
    """
    功能描述：构造 unique_id seed，逻辑对齐仓库 UuidProcessor 默认优先级。
    入参：
      - sample: 当前样本 dict。
      - context: 平台上下文。
      - existing_value: 原始 meta_info.unique_id。
    出参：
      - dict: UUID5 seed payload。
    依赖说明：
      - 依赖 collect_identity_group、has_identity_value、normalize_identity_value。
    """
    preferred_groups = [
        (("meta_info.source_key", "source_key"),),
        (
            ("meta_info.scenario.log_id", "log_id"),
            ("input.message_id", "message_id"),
        ),
        (("meta_info.scenario.log_id", "log_id"),),
        (("input.message_id", "message_id"),),
        (("meta_info.fingerprint", "fingerprint"),),
    ]

    for group in preferred_groups:
        seed_payload = collect_identity_group(sample, group)
        if seed_payload:
            return seed_payload

    if has_identity_value(existing_value):
        return {"existing_unique_id": normalize_identity_value(existing_value)}

    return {
        "dataset": get_context_dataset_name(context),
        "line_number": get_context_line_number(context),
    }


def collect_identity_group(sample, group):
    """
    功能描述：按候选字段组收集 UUID seed；组内字段必须全部存在。
    入参：
      - sample: 当前样本 dict。
      - group: tuple[(path, alias)]。
    出参：
      - dict: seed payload，字段缺失则返回空 dict。
    依赖说明：
      - 依赖 get_by_path、has_identity_value、normalize_identity_value。
    """
    payload = {}

    for path, alias in group:
        value = get_by_path(sample, path)
        if not has_identity_value(value):
            return {}
        payload[alias] = normalize_identity_value(value)

    return payload


def get_by_path(data, path, default=None):
    """
    功能描述：按点分路径读取嵌套 dict。
    入参：
      - data: 待读取 dict。
      - path: 点分路径。
      - default: 缺失默认值。
    出参：
      - object: 读取结果。
    依赖说明：
      - 无外部依赖。
    """
    current = data

    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current


def deep_copy(value):
    """
    功能描述：递归复制 dict/list，避免修改原对象。
    入参：
      - value: 任意 Python 对象。
    出参：
      - object: 复制后的对象。
    依赖说明：
      - 无外部依赖。
    """
    if isinstance(value, dict):
        return {key: deep_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [deep_copy(item) for item in value]
    return value


def normalize_for_fingerprint(value):
    """
    功能描述：规范化 fingerprint 输入，保证同内容稳定生成同一 hash。
    入参：
      - value: 任意字段值。
    出参：
      - object: 规范化后的字段值。
    依赖说明：
      - 无外部依赖。
    """
    if isinstance(value, str):
        return value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if isinstance(value, dict):
        return {key: normalize_for_fingerprint(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_for_fingerprint(item) for item in value]
    return value


def is_uuid_value(value):
    """
    功能描述：判断值是否为标准 UUID 字符串。
    入参：
      - value: 任意值。
    出参：
      - bool: 是 UUID 返回 True。
    依赖说明：
      - 依赖 uuid 标准库。
    """
    if isinstance(value, uuid_lib.UUID):
        return True
    if not isinstance(value, str):
        return False

    text = value.strip()
    if not text:
        return False

    try:
        parsed = uuid_lib.UUID(text)
    except (AttributeError, ValueError, TypeError):
        return False

    return str(parsed) == text.lower()


def has_identity_value(value):
    """
    功能描述：判断字段是否可作为 identity seed。
    入参：
      - value: 任意字段值。
    出参：
      - bool: 有效返回 True。
    依赖说明：
      - 无外部依赖。
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def normalize_identity_value(value):
    """
    功能描述：规范化 identity seed 字段。
    入参：
      - value: 任意字段值。
    出参：
      - object: 字符串去首尾空格，其余原样。
    依赖说明：
      - 无外部依赖。
    """
    if isinstance(value, str):
        return value.strip()
    return value


def get_context_dataset_name(context):
    """
    功能描述：从 context 中读取数据集名称。
    入参：
      - context: 平台上下文，可能是 dict 或对象。
    出参：
      - str: 数据集名称。
    依赖说明：
      - 无外部依赖。
    """
    if isinstance(context, dict):
        input_path = context.get("input_path")
        if isinstance(input_path, dict):
            return str(input_path.get("name", ""))
        if input_path is not None and hasattr(input_path, "name"):
            return str(input_path.name)
        return str(context.get("dataset", ""))

    input_path = getattr(context, "input_path", None)
    if input_path is not None and hasattr(input_path, "name"):
        return str(input_path.name)

    return ""


def get_context_line_number(context):
    """
    功能描述：从 context 中读取数据行号。
    入参：
      - context: 平台上下文，可能是 dict 或对象。
    出参：
      - object: 行号，缺失返回空字符串。
    依赖说明：
      - 无外部依赖。
    """
    if isinstance(context, dict):
        return context.get("dataset_line_number", "")
    return getattr(context, "dataset_line_number", "")
