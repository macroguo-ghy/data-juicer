#!/usr/bin/env python3
"""Submit and inspect the online Ray E2E job through ad.ai.data_forge RPCs."""

import argparse
import copy
import datetime as dt
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = Path("demos/bytedance/e2e_test/e2e_test.yaml")
DEFAULT_IMAGE_URL = "hub.byted.org/ad_stats/data_juicer:ea784a3ddfc181e1c6b1dc717f3250b4"
DEFAULT_NAMESPACE = "/topic/790e3ece1131c882"
DEFAULT_QUEUE = "root.panda_hl_ad_stats_general_h"
DEFAULT_YARN_CLUSTER = "rabbit-hl"
DEFAULT_PROJECT_ID = "paubxt82r1tu"
DEFAULT_ENTRYPOINT_CONFIG = "demos/bytedance/e2e_test/e2e_test.yaml"
DEFAULT_WORK_DIR_TEMPLATE = "./outputs/e2e_test/{job_id}"


def current_branch() -> str:
    try:
        return subprocess.check_output(
            ["git", "branch", "--show-current"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "dev/local-20260514-162151"


def default_job_id() -> str:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"e2e_test_{stamp}"


def bytedcli_login_username(bytedcli: str = "bytedcli", timeout_seconds: int = 5) -> str:
    try:
        completed = subprocess.run(
            [bytedcli, "--json", "auth", "status"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
        )
        if completed.returncode != 0 or not completed.stdout:
            return ""
        data = json.loads(completed.stdout)
    except Exception:
        return ""

    identity = data.get("data", {}).get("bytecloud_auth", {}).get("identity", {})
    username = identity.get("username")
    return username if isinstance(username, str) else ""


def default_username() -> str:
    return (
        bytedcli_login_username()
        or os.environ.get("BYTEDANCE_USERNAME")
        or os.environ.get("BYTE_USER")
        or os.environ.get("USER")
        or ""
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: object) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def first_value_by_key(data: object, key: str):
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            found = first_value_by_key(value, key)
            if found not in (None, ""):
                return found
    elif isinstance(data, list):
        for value in data:
            found = first_value_by_key(value, key)
            if found not in (None, ""):
                return found
    return None


def rpc_body(response: dict) -> dict:
    body = first_value_by_key(response, "resp_body")
    if isinstance(body, str) and body:
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return {}


def redact_operator_yaml(yaml_text: str) -> str:
    sensitive_keys = r"(?:api_key|tqs_app_id|tqs_app_key)"
    return re.sub(rf"(?m)^(\s*{sensitive_keys}\s*:\s*).*$", r'\1"<redacted>"', yaml_text)


def request_for_disk(request: dict, save_sensitive_request: bool) -> dict:
    if save_sensitive_request:
        return request
    sanitized = copy.deepcopy(request)
    if isinstance(sanitized.get("operator_yaml"), str):
        sanitized["operator_yaml"] = redact_operator_yaml(sanitized["operator_yaml"])
    return sanitized


def merge_top_level_scalar(yaml_text: str, key: str, value: str) -> str:
    line = f'{key}: "{value}"'
    pattern = re.compile(rf"^{re.escape(key)}\s*:.*$", re.MULTILINE)
    if pattern.search(yaml_text):
        return pattern.sub(line, yaml_text, count=1)
    return line + "\n" + yaml_text


def replace_yaml_placeholder(yaml_text: str, placeholder: str, value: str, *, config_path: Path, option_hint: str) -> str:
    if placeholder not in yaml_text:
        return yaml_text
    if not value:
        raise SystemExit(f"{config_path} still contains {placeholder}. {option_hint}")
    return yaml_text.replace(placeholder, value)


def prepare_operator_yaml(args) -> str:
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = REPO_ROOT / config_path
    yaml_text = config_path.read_text(encoding="utf-8")

    api_key = args.ark_api_key or os.environ.get(args.ark_api_key_env, "")
    if "<YOUR_API_KEY>" in yaml_text:
        if not api_key and not args.allow_placeholder_api_key:
            raise SystemExit(
                f"{config_path} still contains <YOUR_API_KEY>. "
                f"Pass --ark-api-key or set {args.ark_api_key_env}, "
                "or use --allow-placeholder-api-key for request generation only."
            )
        if api_key:
            yaml_text = yaml_text.replace("<YOUR_API_KEY>", api_key)
    tqs_app_id = args.tqs_app_id or os.environ.get(args.tqs_app_id_env, "")
    tqs_app_key = args.tqs_app_key or os.environ.get(args.tqs_app_key_env, "")
    yaml_text = replace_yaml_placeholder(
        yaml_text,
        "<YOUR_TQS_APP_ID>",
        tqs_app_id,
        config_path=config_path,
        option_hint=f"Pass --tqs-app-id or set {args.tqs_app_id_env}.",
    )
    yaml_text = replace_yaml_placeholder(
        yaml_text,
        "<YOUR_TQS_APP_KEY>",
        tqs_app_key,
        config_path=config_path,
        option_hint=f"Pass --tqs-app-key or set {args.tqs_app_key_env}.",
    )
    if args.model:
        yaml_text = re.sub(r'(?m)^(\s*model\s*:\s*).*$',
                           rf'\1"{args.model}"', yaml_text, count=1)

    yaml_text = merge_top_level_scalar(yaml_text, "job_id", args.job_id)
    if args.work_dir_template:
        yaml_text = merge_top_level_scalar(yaml_text, "work_dir", args.work_dir_template)
    return yaml_text


def user_context(username: str, email: str) -> dict:
    if not username:
        raise SystemExit("username is required. Pass --username or set BYTEDANCE_USERNAME/BYTE_USER/USER.")
    return {
        "username": username,
        "user_role": "",
        "user_email": email or f"{username}@bytedance.com",
    }


def base_context() -> dict:
    return {
        "log_id": "",
        "caller": "",
        "addr": "",
        "client": "",
        "traffic_env": {
            "open": False,
            "env": "",
        },
        "extra": {},
    }


def yarn_resource(args) -> dict:
    return {
        "backend": "YARN",
        "yarn_config": {
            "queue_name": args.yarn_queue,
            "cluster_name": args.yarn_cluster,
            "idc": args.yarn_idc,
            "project_id": args.yarn_project_id,
            "roles": [
                {
                    "name": "worker",
                    "num": args.worker_num,
                    "memory": args.worker_memory,
                    "gpu": 0,
                    "cpu": args.worker_cpu,
                },
                {
                    "name": "head",
                    "num": 1,
                    "memory": args.head_memory,
                    "gpu": 0,
                    "cpu": args.head_cpu,
                },
            ],
        },
    }


def arnold_resource(args) -> dict:
    return {
        "backend": "ARNOLD",
        "arnold_config": {
            "group_ids": [args.arnold_group_id],
            "cluster_id": args.arnold_cluster_id,
            "quota_pool": args.arnold_quota_pool,
            "roles": [
                {
                    "name": "worker",
                    "num": args.arnold_worker_num,
                    "memory": args.arnold_worker_memory,
                    "gpu": args.arnold_gpu,
                    "gpuv": args.arnold_gpuv,
                    "queue_name": args.arnold_queue,
                    "scheduling_options": "{}",
                    "cpu": args.arnold_worker_cpu,
                    "ports": 10,
                    "preemptible": False,
                    "resource_pool": "",
                }
            ],
        },
    }


def launch_request(args) -> dict:
    entrypoint = (
        "python tools/process_data.py "
        f"--config {args.entrypoint_config} "
        f"--job_id {args.job_id}"
    )
    request = {
        "namespace_name": args.namespace,
        "caption": args.caption,
        "description": args.description,
        "tags": args.tags,
        "type": "RayCluster",
        "sub_type": "SingleJob",
        "job_def_version": {
            "image_meta": {
                "image_sid": "",
                "image_vid": "",
                "image_source": "url",
                "image_url": args.image_url,
                "task_id": 0,
                "need_build": False,
            },
            "git_repo": {
                "repo_name": args.repo_name,
                "branch_name": args.branch,
                "tag": "",
                "mnt": args.repo_mnt,
                "commit_sha": args.commit_sha,
                "use_latest_commit": args.use_latest_commit,
            },
            "env": {
                "BYTED_RAY_ray_io_dont_shutdown_cluster_after_job_finished": "true",
                "BYTED_RAY_ray_io_param_head_no_cpu": "true",
                "RAY_max_lineage_bytes": "5368709120",
                "RAY_memory_monitor_refresh_ms": "0",
            },
            "entrypoint_full_script": entrypoint,
        },
        "resources": [yarn_resource(args)],
        "user_context": user_context(args.username, args.user_email),
        "base": base_context(),
    }
    if args.with_arnold:
        request["resources"].append(arnold_resource(args))
    if args.operator_yaml:
        request["operator_yaml"] = prepare_operator_yaml(args)
    return request


def rpc_args(args, method: str, body_file: Path) -> list[str]:
    cmd = [
        args.bytedcli,
        "--json",
        "bits",
        "rpc-call",
        args.psm,
        method,
        "--idl-version",
        args.idl_version,
        "--idl-source",
        "branch",
        "--zone",
        args.zone,
        "--idc",
        args.idc,
        "--env",
        args.env,
        "--cluster",
        args.cluster,
        "--body-file",
        str(body_file),
    ]
    if args.control_plane:
        cmd.extend(["--control-plane", args.control_plane])
    return cmd


def call_rpc(args, method: str, request: dict, out_dir: Path) -> dict:
    ensure_dir(out_dir)
    request_file = out_dir / f"{method}.request.json"
    response_file = out_dir / f"{method}.response.json"
    write_json(request_file, request_for_disk(request, args.save_sensitive_request))

    if args.dry_run:
        print(f"dry-run request: {request_file}")
        return {}

    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as tmp:
        json.dump(request, tmp, ensure_ascii=False)
        tmp_path = Path(tmp.name)
    try:
        completed = subprocess.run(
            rpc_args(args, method, tmp_path),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    response_file.write_text(completed.stdout, encoding="utf-8")
    if completed.stderr:
        (out_dir / f"{method}.stderr.log").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise SystemExit(
            f"{method} failed with exit code {completed.returncode}. "
            f"See {response_file} and {out_dir / (method + '.stderr.log')}."
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise SystemExit(f"{method} returned non-JSON output saved at {response_file}: {err}") from err


def sid_from_args(args) -> str:
    if args.sid:
        return args.sid
    if args.run_dir:
        metadata = read_json(Path(args.run_dir) / "metadata.json")
        sid = metadata.get("sid")
        if sid:
            return sid
    raise SystemExit("sid is required. Pass --sid or --run-dir with metadata.json.")


def federal_job_request(args, sid: str) -> dict:
    return {
        "merlin_federal_job_sid": sid,
        "user_context": user_context(args.username, args.user_email),
        "base": base_context(),
    }


def cmd_launch(args) -> None:
    if args.job_id is None:
        args.job_id = default_job_id()
    if args.caption is None:
        args.caption = args.job_id
    out_dir = Path(args.out_dir or f"/tmp/data_juicer_e2e/{args.job_id}")
    request = launch_request(args)
    response = call_rpc(args, "LaunchMerlinFederalJob", request, out_dir)
    body = rpc_body(response) or response
    sid = first_value_by_key(body, "merlin_federal_job_sid")
    metadata = {
        "sid": sid,
        "job_id": args.job_id,
        "caption": args.caption,
        "config": str(args.config),
        "branch": args.branch,
        "image_url": args.image_url,
        "out_dir": str(out_dir),
    }
    write_json(out_dir / "metadata.json", metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


def cmd_status(args) -> None:
    sid = sid_from_args(args)
    out_dir = Path(args.run_dir or f"/tmp/data_juicer_e2e/{sid}")
    response = call_rpc(args, "GetMerlinFederalJob", federal_job_request(args, sid), out_dir)
    body = rpc_body(response) or response
    detail_json = first_value_by_key(body, "detail_json")
    detail = {}
    if isinstance(detail_json, str) and detail_json:
        try:
            detail = json.loads(detail_json)
        except json.JSONDecodeError:
            detail = {}
    summary = {
        "sid": first_value_by_key(body, "merlin_federal_job_sid") or sid,
        "status": first_value_by_key(body, "status"),
        "detail_status": detail.get("status"),
        "cluster": detail.get("byted_ray_cluster_name"),
        "status_code": first_value_by_key(body, "status_code"),
        "status_message": first_value_by_key(body, "status_message"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def cmd_ray_ui(args) -> None:
    sid = sid_from_args(args)
    out_dir = Path(args.run_dir or f"/tmp/data_juicer_e2e/{sid}")
    response = call_rpc(args, "GetMerlinFederalJobRayUI", federal_job_request(args, sid), out_dir)
    body = rpc_body(response) or response
    url = first_value_by_key(body, "url")
    print(url or json.dumps(response, ensure_ascii=False, indent=2))


def cmd_stop(args) -> None:
    sid = sid_from_args(args)
    out_dir = Path(args.run_dir or f"/tmp/data_juicer_e2e/{sid}")
    request = federal_job_request(args, sid)
    request["action"] = "Stop"
    response = call_rpc(args, "OperateMerlinFederalJob", request, out_dir)
    print(json.dumps(response, ensure_ascii=False, indent=2))


def add_common_rpc_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bytedcli", default=os.environ.get("BYTEDCLI", "bytedcli"))
    parser.add_argument("--psm", default="ad.ai.data_forge")
    parser.add_argument("--idl-version", default="codex/use-python-311")
    parser.add_argument("--zone", default="CN")
    parser.add_argument("--idc", default="hl")
    parser.add_argument("--env", default="ppe_terranova")
    parser.add_argument("--cluster", default="default")
    parser.add_argument("--control-plane", default="")
    parser.add_argument("--username", default=default_username())
    parser.add_argument("--user-email", default="")
    parser.add_argument("--dry-run", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    launch = subparsers.add_parser("launch", help="submit the E2E Ray job")
    add_common_rpc_args(launch)
    launch.add_argument("--config", default=str(DEFAULT_CONFIG))
    launch.add_argument("--operator-yaml", action=argparse.BooleanOptionalAction, default=True)
    launch.add_argument("--ark-api-key", default="")
    launch.add_argument("--ark-api-key-env", default="ARK_API_KEY")
    launch.add_argument("--tqs-app-id", default="")
    launch.add_argument("--tqs-app-id-env", default="TQS_APP_ID")
    launch.add_argument("--tqs-app-key", default="")
    launch.add_argument("--tqs-app-key-env", default="TQS_APP_KEY")
    launch.add_argument("--model", default="")
    launch.add_argument("--allow-placeholder-api-key", action="store_true")
    launch.add_argument("--save-sensitive-request", action="store_true")
    launch.add_argument("--job-id", default=None)
    launch.add_argument("--caption", default=None)
    launch.add_argument("--description", default="")
    launch.add_argument("--tags", nargs="*", default=[])
    launch.add_argument("--out-dir", default="")
    launch.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    launch.add_argument("--image-url", default=DEFAULT_IMAGE_URL)
    launch.add_argument("--repo-name", default="ad/data-juicer")
    launch.add_argument("--branch", default=current_branch())
    launch.add_argument("--repo-mnt", default="/opt/tiger/data-juicer")
    launch.add_argument("--commit-sha", default="")
    launch.add_argument("--use-latest-commit", action=argparse.BooleanOptionalAction, default=True)
    launch.add_argument("--entrypoint-config", default=DEFAULT_ENTRYPOINT_CONFIG)
    launch.add_argument("--work-dir-template", default=DEFAULT_WORK_DIR_TEMPLATE)
    launch.add_argument("--yarn-queue", default=DEFAULT_QUEUE)
    launch.add_argument("--yarn-cluster", default=DEFAULT_YARN_CLUSTER)
    launch.add_argument("--yarn-idc", default="hl")
    launch.add_argument("--yarn-project-id", default=DEFAULT_PROJECT_ID)
    launch.add_argument("--worker-num", type=int, default=10)
    launch.add_argument("--worker-memory", type=int, default=32768)
    launch.add_argument("--worker-cpu", type=int, default=4)
    launch.add_argument("--head-memory", type=int, default=65536)
    launch.add_argument("--head-cpu", type=int, default=8)
    launch.add_argument("--with-arnold", action="store_true")
    launch.add_argument("--arnold-group-id", type=int, default=955)
    launch.add_argument("--arnold-cluster-id", type=int, default=17)
    launch.add_argument("--arnold-quota-pool", default="default")
    launch.add_argument("--arnold-worker-num", type=int, default=1)
    launch.add_argument("--arnold-worker-memory", type=int, default=45056)
    launch.add_argument("--arnold-worker-cpu", type=int, default=11)
    launch.add_argument("--arnold-gpu", type=int, default=1)
    launch.add_argument("--arnold-gpuv", default="A100_SXM_80GB")
    launch.add_argument("--arnold-queue", default="compute-1190-lq-cloudnative-ai-life.alg.genai-guarantee")
    launch.set_defaults(func=cmd_launch)

    for name, help_text, func in [
        ("status", "fetch Federal job status", cmd_status),
        ("ray-ui", "fetch Ray UI URL", cmd_ray_ui),
        ("stop", "stop a Federal job", cmd_stop),
    ]:
        command = subparsers.add_parser(name, help=help_text)
        add_common_rpc_args(command)
        command.add_argument("--sid", default="")
        command.add_argument("--run-dir", default="")
        command.set_defaults(save_sensitive_request=False)
        command.set_defaults(func=func)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
