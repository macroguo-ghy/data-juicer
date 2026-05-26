include "base.thrift"

namespace py toutiao.videoarch.smart_player

enum VideoDefinition {
    ALL = 0
    V360P = 1
    V480P = 2
    V720P = 3
    V1080P = 4
    V240P = 5
    V540P = 6
}

enum UrlType {
   VL0 = 6
   VL1 = 7
   VL2 = 8
   VL3 = 9
   VL4 = 10
   VL5 = 11
}

struct FilterParams {
    1: optional VideoDefinition NeedDefinition = VideoDefinition.ALL
}

struct UrlParams {
    2: optional UrlType UrlType = UrlType.VL0
    4: optional i64 Indate
}

struct Identity {
    1: optional string IdentityInfo
    2: optional string AuthToken
    3: optional string AuthPolicy
}

struct Meta {
    1: i64 Height
    2: i64 Width
    4: double Duration
    7: string Definition
    10: string EncodedType
    16: i64 FPS
    22: string HDRType
}

struct VideoInfo {
   1: string MainUrl
   2: string BackupUrl
   3: Meta VideoMeta
}

struct PlayInfo {
    1: i64 Status
    2: string Message
    10: list<VideoInfo> VideoInfos
    11: optional VideoInfo OriginalVideoInfo
}

struct MGetPlayInfosV2Request {
    1: required list<string> VIDs
    2: optional FilterParams FilterParams
    4: required UrlParams UrlParams
    5: required Identity Identity
    9: optional bool NeedOriginalVideoInfo = true
    255: optional base.Base Base
}

struct MGetPlayInfosV2Response {
    1: map<string, PlayInfo> VideoInfos
    255: optional base.BaseResp BaseResp
}

service SmartPlayerService {
    MGetPlayInfosV2Response MGetPlayInfosV2(1: MGetPlayInfosV2Request request)
}
