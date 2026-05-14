namespace py lab.ocr.general

include "base.thrift"

struct Point {
    1: double x,
    2: double y,
}

struct ImageInfo {
    1: string url,
    2: string tos_bucket,
    3: string tos_obj,
    4: binary data,
    5: i32 frame_no,
}

struct VideoInfo {
    1: string vid,
    2: list<ImageInfo> frames,
}

struct ImagesOcrRequest {
    1: list<ImageInfo> images,
    2: bool detection_only,
    3: bool recognition_only,
    254: map<string, string> extra,
    255: optional base.Base Base,
}

struct PatchOcrResult {
    1: list<Point> det_points_abs,
    2: list<Point> det_points_relative,
    3: string lang,
    4: string text,
    5: double prob,
    251: optional list<Point> curve_vertexes,
    252: optional list<list<i32>> char_poses,
    253: optional list<double> char_probs,
    254: optional map<string, string> extra,
    255: optional list<string> tags,
}

struct ImageOcrResult {
    1: list<PatchOcrResult> words,
    2: i32 frame_no,
    3: optional string status,
    255: optional map<string, string> extra,
}

struct OcrResult {
    1: list<ImageOcrResult> results,
    255: optional base.BaseResp BaseResp,
}

service OcrService {
    OcrResult PredictImages(1: ImagesOcrRequest req)
    OcrResult SafePredictImages(1: ImagesOcrRequest req)
}
