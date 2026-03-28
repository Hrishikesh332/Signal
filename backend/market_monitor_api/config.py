from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    app_name: str
    source_config_file: str
    snapshot_store_dir: str
    source_run_store_dir: str
    tinyfish_base_url: str
    tinyfish_api_key: str
    tinyfish_timeout_seconds: int
    openai_base_url: str
    openai_api_key: str
    openai_model: str
    openai_timeout_seconds: int
    project_root: str
    env_file: str

    @property
    def tinyfish_configured(self) -> bool:
        return bool(self.tinyfish_base_url and self.tinyfish_api_key)

    @property
    def openai_configured(self) -> bool:
        return bool(self.openai_base_url and self.openai_api_key and self.openai_model)

    def resolve_path(self, raw_path: str) -> Path:
        candidate = Path(raw_path)
        if candidate.is_absolute():
            return candidate
        return Path(self.project_root) / candidate


def parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key, value = stripped.split("=", 1)
    normalized_value = value.strip().strip('"').strip("'")
    return key.strip(), normalized_value


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        parsed = parse_env_line(raw_line)
        if not parsed:
            continue
        key, value = parsed
        os.environ.setdefault(key, value)


def get_env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default)).strip()
    try:
        return int(raw_value)
    except ValueError:
        return default


@lru_cache(maxsize=1)
def get_settings(project_root: Path | None = None) -> Settings:
    resolved_root = (project_root or Path.cwd()).resolve()
    env_path = resolved_root / ".env"
    load_env_file(env_path)
    return Settings(
        app_name=os.environ.get("MARKET_MONITOR_APP_NAME", "AI Market Sentry Platform").strip(),
        source_config_file=os.environ.get(
            "MARKET_MONITOR_SOURCE_CONFIG_FILE",
            "backend/config/sources.json",
        ).strip(),
        snapshot_store_dir=os.environ.get(
            "MARKET_MONITOR_SNAPSHOT_STORE_DIR",
            "backend/data/snapshots",
        ).strip(),
        source_run_store_dir=os.environ.get(
            "MARKET_MONITOR_SOURCE_RUN_STORE_DIR",
            "backend/data/source_runs",
        ).strip(),
        tinyfish_base_url=os.environ.get("TINYFISH_BASE_URL", "https://agent.tinyfish.ai").strip(),
        tinyfish_api_key=os.environ.get("TINYFISH_API_KEY", "").strip(),
        tinyfish_timeout_seconds=get_env_int("TINYFISH_TIMEOUT_SECONDS", 30),
        openai_base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").strip(),
        openai_api_key=os.environ.get("OPENAI_API_KEY", "").strip(),
        openai_model=os.environ.get("OPENAI_MODEL", "").strip(),
        openai_timeout_seconds=get_env_int("OPENAI_TIMEOUT_SECONDS", 30),
        project_root=str(resolved_root),
        env_file=str(env_path),
    )
