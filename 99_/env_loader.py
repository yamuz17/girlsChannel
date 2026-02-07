from __future__ import annotations

import os
import inspect
import json
from pathlib import Path
from typing import Optional, Dict, Any

# python-dotenv が入っていれば最優先で使う
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None


def _guess_caller_file() -> Path:
    """
    env_loader.load_env() が引数なしで呼ばれた場合に、
    呼び出し元スクリプトのパスを推定する。
    """
    for frame in inspect.stack()[1:]:
        p = Path(frame.filename)
        # env_loader自身は除外
        if p.name != "env_loader.py":
            return p
    # 最後の保険
    return Path.cwd() / "dummy.py"


def _parse_env_file(path: Path) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # 変数展開 & ~ 展開
        v = os.path.expandvars(v)
        v = str(Path(v).expanduser())
        d[k] = v
    return d


def _default_values() -> Dict[str, str]:
    base = Path.home() / "Documents" / "pythonOutput"
    return {
        "DB_PATH": str(base / "list_category_gossip.db"),
        "BASE_OUTPUT_ROOT": str(base),
    }


def _load_config_local(path: Path) -> Dict[str, str]:
    """
    config.local.json を読み込み、キー/値（文字列）を返す。
    """
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in data.items():
            if v is None:
                continue
            out[str(k)] = str(v)
        return out
    except Exception:
        return {}


def _common_env_path(filename: str) -> Path:
    """
    リポジトリ共通の .env（別リポジトリでも同じ場所を見る）
    """
    return Path.home() / "Documents" / "readOnly" / filename


def load_env(path: Optional[Path] = None, filename: str = "girlsChannel.env") -> Dict[str, str]:
    """
    .env を読み込んで os.environ に反映し、読み込んだキーを dict で返す。
    - path が None の場合：呼び出し元スクリプトと同階層の .env を読む
    - path がファイルならそれを読む
    - path がディレクトリなら path/filename を読む
    """
    if path is None:
        caller = _guess_caller_file()
        # 1つ上の階層の env を優先
        candidate = caller.resolve().parent.parent / filename
        # 無ければローカルDocuments/readOnly/{filename}
        env_path = candidate if candidate.exists() else _common_env_path(filename)
    else:
        p = Path(path).expanduser()
        env_path = p if p.is_file() else (p / filename)

    config_local_path = env_path.parent / "config.local.json"
    config_local = _load_config_local(config_local_path)

    env_file: Dict[str, str] = {}
    if env_path.exists():
        env_file = _parse_env_file(env_path)

    # 返り値は .env の内容（従来互換）＋ config.local を含めたもの
    d: Dict[str, str] = {}
    d.update(env_file)
    d.update(config_local)

    # 優先順位: 既存環境変数 > config.local.json > .env > defaults
    merged: Dict[str, str] = {}
    merged.update(_default_values())
    merged.update(env_file)
    merged.update(config_local)
    for k, v in merged.items():
        os.environ.setdefault(k, v)

    # DB_PATH の親ディレクトリは自動作成（ローカル運用の利便性向上）
    db_path = merged.get("DB_PATH")
    if db_path:
        try:
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    # python-dotenv があるなら、念のため読み込み（export 形式等の互換）
    if load_dotenv is not None and env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)

    return d


def load_env_next_to_script(script_file: str, filename: str = ".env") -> Dict[str, str]:
    base = Path(script_file).resolve().parent
    return load_env(base / filename)


# =========================
# env getters（01/02/99互換）
# =========================
def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)

def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return int(default)
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return int(default)
    return int(float(s))

def env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None:
        return float(default)
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return float(default)
    return float(s)

def env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)

def env_optional_int(name: str, default: Optional[int]) -> Optional[int]:
    v = os.environ.get(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return default
    try:
        return int(float(s))
    except Exception:
        return default

def env_path(name: str, default: Optional[str]) -> Optional[Path]:
    v = os.environ.get(name)
    if v is None:
        return None if default is None else Path(default).expanduser()
    s = str(v).strip()
    if not s:
        return None if default is None else Path(default).expanduser()
    return Path(s).expanduser()
