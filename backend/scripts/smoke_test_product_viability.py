#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
import socket
import sys
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from market_monitor_api.config import load_env_file  # noqa: E402


DEFAULT_SAMPLE_REQUEST = {
    "query": (
        "Would a portable espresso maker for travelers and campers be commercially viable? "
        "Assume a premium consumer hardware product around $79 for coffee enthusiasts who want "
        "cafe-style coffee without carrying bulky equipment."
    ),
    "research_depth": "standard",
}


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    env_path = REPO_ROOT / ".env"
    load_env_file(env_path)
    missing_env = find_missing_env_keys()
    if missing_env:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "missing_env",
                    "message": f"Missing required keys in {env_path}: {', '.join(missing_env)}",
                },
                indent=2,
            )
        )
        return 1

    args = apply_default_request_values(args)
    args.timeout_seconds = resolve_timeout_seconds(args.timeout_seconds)
    print_request_summary(args)

    request_fields = build_request_fields(args)
    request_files = build_request_files(args.image)
    body, content_type = encode_multipart_formdata(request_fields, request_files)

    request = Request(
        args.url,
        data=body,
        headers={"Content-Type": content_type},
        method="POST",
    )
    try:
        with urlopen(request, timeout=args.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
            print(json.dumps(payload, indent=2))
            return 0
    except HTTPError as exc:
        payload = read_error_payload(exc)
        print(json.dumps(payload, indent=2))
        return 1
    except URLError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "network_error",
                    "message": str(exc.reason),
                },
                indent=2,
            )
        )
        return 1
    except (TimeoutError, socket.timeout):
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "client_timeout",
                    "message": (
                        "The smoke test client timed out while waiting for the backend response. "
                        f"Current timeout: {args.timeout_seconds} seconds."
                    ),
                },
                indent=2,
            )
        )
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a real multipart smoke-test request to /api/v1/product-viability.",
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:5000/api/v1/product-viability",
        help="Endpoint URL. Default: http://127.0.0.1:5000/api/v1/product-viability",
    )
    parser.add_argument("--query", default="", help="Natural-language product viability request")
    parser.add_argument("--product-name", default="", help="Product name")
    parser.add_argument("--description", default="", help="Product description")
    parser.add_argument("--category", default="", help="Product category")
    parser.add_argument("--price-point", default="", help="Price point")
    parser.add_argument("--target-customer", default="", help="Target customer")
    parser.add_argument("--market-context", default="", help="Market context")
    parser.add_argument(
        "--research-depth",
        choices=["standard", "deep"],
        default="standard",
        help="Research depth. Default: standard",
    )
    parser.add_argument(
        "--image",
        action="append",
        default=[],
        help="Image path to include. Repeat --image for multiple files.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="HTTP timeout in seconds. Default: derived from .env TinyFish/OpenAI timeouts.",
    )
    parser.add_argument(
        "--no-defaults",
        action="store_true",
        help="Disable the built-in sample payload and require explicit request fields.",
    )
    return parser


def apply_default_request_values(args):
    if args.no_defaults:
        return args

    for field_name, default_value in DEFAULT_SAMPLE_REQUEST.items():
        current_value = getattr(args, field_name.replace("-", "_"), None)
        if not current_value:
            setattr(args, field_name, default_value)
    return args


def build_request_fields(args) -> list[tuple[str, str]]:
    fields = [
        ("query", args.query),
        ("product_name", args.product_name),
        ("description", args.description),
        ("category", args.category),
        ("price_point", args.price_point),
        ("target_customer", args.target_customer),
        ("market_context", args.market_context),
        ("research_depth", args.research_depth),
    ]
    return [(name, value) for name, value in fields if value]


def print_request_summary(args) -> None:
    summary = {
        "url": args.url,
        "query": args.query or None,
        "product_name": args.product_name,
        "research_depth": args.research_depth,
        "category": args.category or None,
        "price_point": args.price_point or None,
        "target_customer": args.target_customer or None,
        "image_count": len(args.image),
        "timeout_seconds": args.timeout_seconds,
        "using_default_payload": not args.no_defaults,
    }
    print(json.dumps({"request": summary}, indent=2))


def build_request_files(image_paths: list[str]) -> list[tuple[str, str, bytes, str]]:
    files = []
    for raw_path in image_paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise SystemExit(f"Image file not found: {path}")
        content = path.read_bytes()
        mime_type, _ = mimetypes.guess_type(path.name)
        files.append(("images", path.name, content, mime_type or "application/octet-stream"))
    return files


def encode_multipart_formdata(
    fields: list[tuple[str, str]],
    files: list[tuple[str, str, bytes, str]],
) -> tuple[bytes, str]:
    boundary = f"----SignalProductViability{uuid.uuid4().hex}"
    lines: list[bytes] = []

    for name, value in fields:
        lines.extend(
            [
                f"--{boundary}".encode("utf-8"),
                f'Content-Disposition: form-data; name="{name}"'.encode("utf-8"),
                b"",
                value.encode("utf-8"),
            ]
        )

    for field_name, filename, content, mime_type in files:
        lines.extend(
            [
                f"--{boundary}".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"'
                ).encode("utf-8"),
                f"Content-Type: {mime_type}".encode("utf-8"),
                b"",
                content,
            ]
        )

    lines.append(f"--{boundary}--".encode("utf-8"))
    lines.append(b"")
    body = b"\r\n".join(lines)
    return body, f"multipart/form-data; boundary={boundary}"


def find_missing_env_keys() -> list[str]:
    required = ["TINYFISH_API_KEY"]
    missing = []
    for key in required:
        value = os.environ.get(key, "").strip()
        if not value:
            missing.append(key)
    return missing


def resolve_timeout_seconds(cli_timeout_seconds: int) -> int:
    if cli_timeout_seconds and cli_timeout_seconds > 0:
        return cli_timeout_seconds

    tinyfish_timeout = get_env_int("TINYFISH_TIMEOUT_SECONDS", 300)
    openai_timeout = get_env_int("OPENAI_TIMEOUT_SECONDS", 120)
    derived_timeout = (tinyfish_timeout * 3) + openai_timeout + 60
    return max(derived_timeout, 900)


def get_env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name, "").strip()
    try:
        return int(raw_value) if raw_value else default
    except ValueError:
        return default


def read_error_payload(error: HTTPError) -> dict:
    try:
        return json.loads(error.read().decode("utf-8"))
    except Exception:
        return {
            "ok": False,
            "error": "http_error",
            "status": error.code,
            "message": error.reason,
        }


if __name__ == "__main__":
    raise SystemExit(main())
