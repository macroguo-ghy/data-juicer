include "base.thrift"

namespace py aweme.pack.url

struct PackImageUrlRequest {
    1: optional i64 appid,
    2: required string uri,
    3: optional string format,
    4: optional string tpl,
    5: optional i64 image_expire_second,
    6: optional i64 width,
    7: optional i64 height,
    8: optional bool use_origin,
    255: optional base.Base Base,
}

struct PackImageUrlResponse {
    1: required string uri,
    2: required list<string> url_list,
    255: optional base.BaseResp BaseResp,
}

struct PackImageUrlParam {
    1: optional i64 appid,
    2: required string uri,
    3: optional string format,
    4: optional string tpl,
    5: optional i64 image_expire_second,
    6: optional i64 width,
    7: optional i64 height,
    8: optional bool use_origin,
}

struct PackUrlResult {
    1: required string uri,
    2: required list<string> url_list,
}

struct BatchPackImageUrlRequest {
    1: list<PackImageUrlParam> pack_params,
    255: optional base.Base Base,
}

struct BatchPackImageUrlResponse {
    1: list<PackUrlResult> pack_results,
    255: optional base.BaseResp BaseResp,
}

struct PackFileUrlRequest {
    1: optional i64 appid,
    2: required string uri,
    3: optional i64 file_expire_second,
    255: optional base.Base Base,
}

struct PackFileUrlResponse {
    1: required string uri,
    2: required list<string> url_list,
    255: optional base.BaseResp BaseResp,
}

service PackUrlService {
    PackImageUrlResponse PackImage(1: PackImageUrlRequest req),
    BatchPackImageUrlResponse BatchPackImage(1: BatchPackImageUrlRequest req),
    PackFileUrlResponse PackFile(1: PackFileUrlRequest req),
}
