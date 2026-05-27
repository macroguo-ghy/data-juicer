import argparse
import base64
import os
import tempfile

from loguru import logger


def decode_base64_config(encoded_config: str) -> str:
    normalized = "".join(encoded_config.split())
    padding = (-len(normalized)) % 4
    normalized = normalized + ("=" * padding)
    return base64.b64decode(normalized, validate=True).decode("utf-8")


def get_process_data_run():
    from data_juicer.tools.process_data import run

    return run


@logger.catch(reraise=True)
def main():
    parser = argparse.ArgumentParser(
        description="Run Data-Juicer with a base64-encoded YAML config.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--config_base64",
        "--config-base64",
        default=os.getenv("DJ_CONFIG_BASE64"),
        help="Base64-encoded Data-Juicer YAML config. Defaults to DJ_CONFIG_BASE64.",
    )

    wrapper_args, dj_args = parser.parse_known_args()
    if not wrapper_args.config_base64:
        parser.error("--config_base64 is required unless DJ_CONFIG_BASE64 is set")
    if "--config" in dj_args or any(arg.startswith("--config=") for arg in dj_args):
        parser.error("--config cannot be used with --config_base64")

    config_content = decode_base64_config(wrapper_args.config_base64)
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", encoding="utf-8", delete=True) as config_file:
        config_file.write(config_content)
        config_file.flush()
        run = get_process_data_run()
        run(["--config", config_file.name] + dj_args)


if __name__ == "__main__":
    main()
