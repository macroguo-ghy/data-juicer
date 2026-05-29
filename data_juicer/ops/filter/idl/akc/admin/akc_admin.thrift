namespace java com.bytedance.ad.dataplatform.sirius.akc.admin.service
include "../base.thrift"

service AkcAdminService {
    AckSearchFilterResponse filter(1: AckSearchFilterRequest request),
}

struct AckSearchFilterRequest {
    1: optional AckSearchCondition condition,
    2: optional list<string> keywords,
    3: optional list<Identifier> identifiers,

    254: required base.BizReq BizReq,
    255: optional base.Base Base,
}

struct Identifier {
    1: string identifier,
    2: string source,
}

struct AckSearchFilterResponse {
    1: list<Identifier> identifiers,

    245: base.BizResp BizResp,
    255: base.BaseResp BaseResp,
}

union AckSearchValue {
    1: string stringValue,
    2: i64 longValue,
    3: bool boolValue,
    4: list<string> stringListValue,
}

struct AckSearchPredicate {
    1: required string field,
    2: required string operator,
    3: optional AckSearchValue value,
}

struct AckSearchCondition {
    1: optional string op,
    2: optional list<AckSearchCondition> children,
    3: optional AckSearchPredicate predicate,
}
