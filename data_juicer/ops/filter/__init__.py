from __future__ import annotations

import importlib
import re
from functools import lru_cache
from pathlib import Path

__all__ = [
    "AlphanumericFilter",
    "AudioDurationFilter",
    "AudioNMFSNRFilter",
    "AudioSizeFilter",
    "AverageLineLengthFilter",
    "CharacterRepetitionFilter",
    "FlaggedWordFilter",
    "ImageAestheticsFilter",
    "ImageAspectRatioFilter",
    "ImageFaceCountFilter",
    "ImageFaceRatioFilter",
    "ImageNSFWFilter",
    "ImagePairSimilarityFilter",
    "ImageShapeFilter",
    "ImageSizeFilter",
    "ImageSubplotFilter",
    "ImageTextMatchingFilter",
    "ImageTextSimilarityFilter",
    "ImageWatermarkFilter",
    "LanguageIDScoreFilter",
    "InContextInfluenceFilter",
    "InstructionFollowingDifficultyFilter",
    "LLMAnalysisFilter",
    "LLMQualityScoreFilter",
    "LLMPerplexityFilter",
    "LLMDifficultyScoreFilter",
    "LLMTaskRelevanceFilter",
    "MaximumLineLengthFilter",
    "PerplexityFilter",
    "PhraseGroundingRecallFilter",
    "SpecialCharactersFilter",
    "SpecifiedFieldFilter",
    "SpecifiedNumericFieldFilter",
    "StopWordsFilter",
    "SuffixFilter",
    "TextActionFilter",
    "TextEmbdSimilarityFilter",
    "TextEntityDependencyFilter",
    "TextLengthFilter",
    "TextPairSimilarityFilter",
    "TokenNumFilter",
    "VideoAestheticsFilter",
    "VideoAspectRatioFilter",
    "VideoDurationFilter",
    "VideoFramesTextSimilarityFilter",
    "VideoMotionScoreFilter",
    "VideoMotionScorePtlflowFilter",
    "VideoMotionScoreRaftFilter",
    "VideoNSFWFilter",
    "VideoOcrAreaRatioFilter",
    "VideoResolutionFilter",
    "VideoTaggingFromFramesFilter",
    "VideoWatermarkFilter",
    "WordRepetitionFilter",
    "WordsNumFilter",
    "GeneralFieldFilter",
]

NON_STATS_FILTERS = [
    "suffix_filter",
    "video_tagging_from_frames_filter",
]

_FILTER_ROOT = Path(__file__).resolve().parent


@lru_cache(maxsize=1)
def _class_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for module_path in _FILTER_ROOT.glob("*.py"):
        if module_path.name == "__init__.py" or module_path.name.startswith("_"):
            continue
        content = module_path.read_text(encoding="utf-8")
        for match in re.finditer(r"^class\s+([A-Za-z_][A-Za-z0-9_]*)\b", content, re.MULTILINE):
            index[match.group(1)] = f".{module_path.stem}"
    return index


def __getattr__(name: str):
    module_name = _class_index().get(name)
    if module_name is None:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    value = getattr(importlib.import_module(module_name, __name__), name)
    globals()[name] = value
    return value
