import argparse
import base64
import os
import sys
import tempfile
from pathlib import Path

from loguru import logger

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def decode_base64_config(encoded_config: str) -> str:
    normalized = "".join(encoded_config.split())
    padding = (-len(normalized)) % 4
    normalized = normalized + ("=" * padding)
    return base64.b64decode(normalized, validate=True).decode("utf-8")


def get_debug_operator_pipeline_run():
    try:
        from tools.debug_operator_pipeline import run
    except ImportError:
        from data_juicer.tools.debug_operator_pipeline import run
    return run


@logger.catch(reraise=True)
def main():
    parser = argparse.ArgumentParser(
        description="Run Data-Juicer operator pipeline debug with a base64-encoded YAML config.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--config_base64",
        "--config-base64",
        default=os.getenv("DJ_DEBUG_CONFIG_BASE64"),
        help="Base64-encoded Data-Juicer debug YAML config. Defaults to DJ_DEBUG_CONFIG_BASE64.",
    )

    wrapper_args, dj_args = parser.parse_known_args()
    if not wrapper_args.config_base64:
        parser.error("--config_base64 is required unless DJ_DEBUG_CONFIG_BASE64 is set")
    if any(arg == "--config" or arg.startswith("--config=") for arg in dj_args):
        parser.error("--config cannot be used with --config_base64")

    config_content = decode_base64_config(wrapper_args.config_base64)
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", encoding="utf-8", delete=True) as config_file:
        config_file.write(config_content)
        config_file.flush()
        run = get_debug_operator_pipeline_run()
        raise SystemExit(run(["--config", config_file.name] + dj_args))


if __name__ == "__main__":
    main()
