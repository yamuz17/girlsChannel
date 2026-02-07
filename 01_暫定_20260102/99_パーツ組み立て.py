#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import wave
import sqlite3
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from pathlib import Path
import env_loader


# =========================================================
# .env 読み込み（このスクリプトと同階層の .env）
# =========================================================


SCRIPT_DIR = Path(__file__).resolve().parent
_ENV_PATH = env_loader.load_env(SCRIPT_DIR / ".env")


# =========================================================
# env helper（none対応など99で必要な分だけ）
# =========================================================
def env_float(name: str, default: float) -> float:
    s = os.environ.get(name, None)
    if s is None:
        return float(default)
    t = str(s).strip().lower()
    if t in ("", "none", "null"):
        return float(default)
    return float(t)


def now_jst() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# 設定（共通envキーを優先）
# =========================================================

# --- 共通：DB / パス ---
DB_PATH = env_loader.env_path("DB_PATH", None)
if DB_PATH is None:
    raise RuntimeError("DB_PATH が .env にありません（必須）")

TABLE_NAME = env_loader.env_str("TABLE_NAME", "items") or "items"

BASE_OUTPUT_ROOT = env_loader.env_path("BASE_OUTPUT_ROOT", None)
if BASE_OUTPUT_ROOT is None:
    raise RuntimeError("BASE_OUTPUT_ROOT が .env にありません（必須）")

# --- 共通：SQLite運用（01/02と合わせる） ---
BUSY_TIMEOUT_MS = env_loader.env_int("BUSY_TIMEOUT_MS", 60000)
SQLITE_JOURNAL_MODE = (env_loader.env_str("SQLITE_JOURNAL_MODE", "WAL") or "WAL").strip()
SQLITE_SYNCHRONOUS = (env_loader.env_str("SQLITE_SYNCHRONOUS", "NORMAL") or "NORMAL").strip()

# --- 共通：DBロック運用 ---
LOCK_RETRY_MAX = env_loader.env_int("LOCK_RETRY_MAX", 25)
LOCK_RETRY_SLEEP_SEC = env_float("LOCK_RETRY_SLEEP_SEC", 0.8)

# --- 共通：ステージ運用（STA/END方式） ---
STA_99 = env_loader.env_int("STA_99", 5)
END_99 = env_loader.env_int("END_99", 6)

# --- 共通：ピック順 ---
PICK_ORDER_99 = (env_loader.env_str("PICK_ORDER_99", "") or "").strip()
if not PICK_ORDER_99:
    PICK_ORDER_99 = (env_loader.env_str("PICK_ORDER", "post_date_desc") or "post_date_desc").strip()

# --- 99（動画組み立て）固有 ---
FPS = env_loader.env_int("FPS", 30)
AUTO_BUILD_AUDIO_DESC = env_loader.env_bool("AUTO_BUILD_AUDIO_DESC", True)
WRITE_TO_LOCAL_TMP = env_loader.env_bool("WRITE_TO_LOCAL_TMP", True)
ENABLE_FASTSTART = env_loader.env_bool("ENABLE_FASTSTART", False)

# tmp掃除
CLEANUP_TMP = env_loader.env_bool("CLEANUP_TMP", True)

# duration下限（短すぎるとffmpegが不安定になるのを避ける）
MIN_SEG_SEC = env_float("MIN_SEG_SEC", 0.05)  # 0.05〜0.10 推奨

# Preview（冒頭に付ける）
ENABLE_PREVIEW = env_loader.env_bool("ENABLE_PREVIEW", True)
PREVIEW_REL = Path(env_loader.env_str("PREVIEW_REL", "image/preview/preview.mp4") or "image/preview/preview.mp4")
PREVIEW_REQUIRED = env_loader.env_bool("PREVIEW_REQUIRED", True)

# Ending（最後に付ける締め）
ENABLE_ENDING = env_loader.env_bool("ENABLE_ENDING", True)
ENDING_IMAGE_PATH = env_loader.env_path("ENDING_IMAGE_PATH", str(BASE_OUTPUT_ROOT / "image/last_01.png"))
ENDING_AUDIO_PATH = env_loader.env_path("ENDING_AUDIO_PATH", str(BASE_OUTPUT_ROOT / "last/last.wav"))
ENDING_PAD_SEC = env_float("ENDING_PAD_SEC", 0.0)

# BGM
ENABLE_BGM = env_loader.env_bool("ENABLE_BGM", True)
BGM_ODD_PATH = env_loader.env_path("BGM_ODD_PATH", str(BASE_OUTPUT_ROOT / "bgm/1.mp3"))
BGM_EVEN_PATH = env_loader.env_path("BGM_EVEN_PATH", str(BASE_OUTPUT_ROOT / "bgm/2.mp3"))
BGM_VOLUME = env_float("BGM_VOLUME", 0.12)
BGM_DUCKING = env_loader.env_bool("BGM_DUCKING", True)
BGM_FADE_SEC = env_float("BGM_FADE_SEC", 0.30)
BGM_START_SEC_ODD = env_float("BGM_START_SEC_ODD", 0.0)
BGM_START_SEC_EVEN = env_float("BGM_START_SEC_EVEN", 0.0)

# concat時「音声無しmp4」が混じっても落ちないようにする
ENSURE_AUDIO_FOR_CONCAT = env_loader.env_bool("ENSURE_AUDIO_FOR_CONCAT", True)
SILENCE_SR = env_loader.env_int("SILENCE_SR", 48000)
SILENCE_CH = env_loader.env_int("SILENCE_CH", 2)
SILENCE_BITRATE = (env_loader.env_str("SILENCE_BITRATE", "192k") or "192k").strip()

# =========================
# 固定（ロジック寄りはスクリプト側）
# =========================
TMP_DIR = Path("/tmp")
W, H = 1080, 1920

Y_TITLE = (0, 384)
Y_MAIN = (384, 960)
Y_COMMENT = (960, 1728)

SIZE_TITLE = (W, Y_TITLE[1] - Y_TITLE[0])        # 1080x384
SIZE_MAIN = (W, Y_MAIN[1] - Y_MAIN[0])           # 1080x576
SIZE_COMMENT = (W, Y_COMMENT[1] - Y_COMMENT[0])  # 1080x768

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
SEG_RE = re.compile(r"^(\d+)_(\d+)ms\.wav$", re.IGNORECASE)
IMG_RE = re.compile(r"^(\d+)\.(png|jpg|jpeg|webp)$", re.IGNORECASE)


# =========================
# DBユーティリティ（01/02と同じ考え方）
# =========================
def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {db_path}")

    con = sqlite3.connect(str(db_path), timeout=BUSY_TIMEOUT_MS / 1000)
    con.row_factory = sqlite3.Row

    con.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE};")
    con.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS};")
    con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
    return con


def ensure_columns(con: sqlite3.Connection) -> None:
    cols_lower = {str(r[1]).lower() for r in con.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()}

    need = {
        "check_create": "INTEGER NOT NULL DEFAULT 0",
        "folder_name": "TEXT",
        "last_error": "TEXT",
        "updated_at": "TEXT",

        "video_created": "INTEGER NOT NULL DEFAULT 0",
        "video_created_at": "TEXT",
        "video_uploaded": "INTEGER NOT NULL DEFAULT 0",
        "video_uploaded_at": "TEXT",
    }

    for name, ddl in need.items():
        if name.lower() not in cols_lower:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {name} {ddl};")
            cols_lower.add(name.lower())

    con.commit()


def pick_one(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    # folder_name 必須（99は素材フォルダ前提）
    order_sql = "id DESC"
    if PICK_ORDER_99 == "post_date_desc":
        order_sql = "post_date DESC, id DESC"
    elif PICK_ORDER_99 == "comments_desc":
        cols_lower = {str(r[1]).lower() for r in con.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()}
        if "comments_count" in cols_lower:
            order_sql = "comments_count DESC, id DESC"
        else:
            order_sql = "id DESC"

    sql = f"""
        SELECT *
          FROM {TABLE_NAME}
         WHERE check_create = ?
           AND folder_name IS NOT NULL
           AND folder_name != ''
         ORDER BY {order_sql}
         LIMIT 1
    """
    return con.execute(sql, (int(STA_99),)).fetchone()


def claim_job_atomic(con: sqlite3.Connection, item_id: int) -> bool:
    """
    99単体を複数プロセスで起動しても二重処理しにくくするための軽い「取り込み」。
    ステージ値は変えず、updated_at をBEGIN IMMEDIATEで更新して rowcount を見る。
    """
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            cur = con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET updated_at = ?
                 WHERE id = ?
                   AND check_create = ?
                """,
                (now_jst(), int(item_id), int(STA_99)),
            )
            con.execute("COMMIT;")
            return cur.rowcount == 1
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on claim_job_atomic")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on claim_job_atomic")


def update_stage_success(con: sqlite3.Connection, item_id: int) -> None:
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            cur = con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET check_create     = ?,
                       last_error       = NULL,
                       updated_at       = ?,
                       video_created    = 1,
                       video_created_at = ?
                 WHERE id = ?
                   AND check_create = ?
                """,
                (int(END_99), now_jst(), now_jst(), int(item_id), int(STA_99)),
            )
            con.execute("COMMIT;")
            if cur.rowcount == 0:
                raise RuntimeError(f"update_success rowcount=0: id={item_id} check_createがSTA_99({STA_99})ではない可能性")
            return
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on update_success")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on update_success")


def update_stage_error(con: sqlite3.Connection, item_id: int, err: str) -> None:
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET last_error = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (err[:2000], now_jst(), int(item_id)),
            )
            con.execute("COMMIT;")
            return
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on update_error")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on update_error")


# =========================
# BGM選択（IDの奇数/偶数 + 開始秒）
# =========================
def pick_bgm_by_id(item_id: int) -> Tuple[Optional[Path], float]:
    if not ENABLE_BGM:
        return (None, 0.0)

    if (item_id % 2) == 1:
        bgm = BGM_ODD_PATH
        start_sec = float(BGM_START_SEC_ODD)
    else:
        bgm = BGM_EVEN_PATH
        start_sec = float(BGM_START_SEC_EVEN)

    if not bgm.exists():
        print(f"[WARN] BGM not found: {bgm}")
        return (None, 0.0)

    return (bgm, max(0.0, start_sec))


# =========================
# ffmpeg / ffprobe ユーティリティ
# =========================
def die(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise RuntimeError(msg)


def ensure_tools() -> None:
    if shutil.which("ffmpeg") is None:
        die("ffmpeg が見つかりません（brew install ffmpeg 等）。")
    if shutil.which("ffprobe") is None:
        # ffmpeg に同梱が一般的だが、万一無い場合は音声検査を無効化する
        print("[WARN] ffprobe が見つかりません。ENSURE_AUDIO_FOR_CONCAT を無効扱いにします。")


def run(cmd: List[str], log_path: Path) -> None:
    print("[CMD]", " ".join(cmd))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = p.stdout or ""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(out, encoding="utf-8")
    if p.returncode != 0:
        print(out)
        raise RuntimeError("Command failed")


def run_capture(cmd: List[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return (p.stdout or "").strip()


def ffprobe_has_audio(mp4: Path) -> bool:
    if shutil.which("ffprobe") is None:
        return True  # 判定できないので「ある前提」にする（落ちたらそこで分かる）
    try:
        out = run_capture([
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "default=nw=1:nk=1",
            str(mp4),
        ])
        return bool(out.strip())
    except Exception:
        return False


def ffprobe_duration_sec(path: Path) -> float:
    if shutil.which("ffprobe") is None:
        return 0.0
    try:
        out = run_capture([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1",
            str(path),
        ])
        return float(out) if out else 0.0
    except Exception:
        return 0.0


def ensure_audio_track(in_mp4: Path, out_mp4: Path, log_path: Path) -> Path:
    """
    音声が無いmp4を検出したら、無音AACを付与して出す（concatで落ちないようにする）。
    """
    if not ENSURE_AUDIO_FOR_CONCAT:
        return in_mp4

    if ffprobe_has_audio(in_mp4):
        return in_mp4

    dur = ffprobe_duration_sec(in_mp4)
    if dur <= 0:
        # durationが取れないなら、とりあえず短い無音を付ける（concatのため）
        dur = 1.0

    ch_layout = "stereo" if int(SILENCE_CH) == 2 else "mono"
    cmd = [
        "ffmpeg", "-y",
        "-i", str(in_mp4),
        "-f", "lavfi", "-t", f"{dur:.6f}",
        "-i", f"anullsrc=channel_layout={ch_layout}:sample_rate={int(SILENCE_SR)}",
        "-shortest",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", SILENCE_BITRATE,
        "-ar", str(int(SILENCE_SR)),
        "-ac", str(int(SILENCE_CH)),
    ]
    if ENABLE_FASTSTART:
        cmd += ["-movflags", "+faststart"]
    cmd += [str(out_mp4)]

    print(f"[INFO] audio missing -> add silent track: {in_mp4.name} (dur={dur:.2f}s)")
    run(cmd, log_path=log_path)
    return out_mp4


def audio_duration_sec_wav(path: Path) -> float:
    if not path.exists():
        die(f"音声が見つかりません: {path}")
    with wave.open(str(path), "rb") as wf:
        frames = wf.getnframes()
        rate = wf.getframerate()
        return 0.0 if rate == 0 else frames / float(rate)


# =========================
# concat list 生成（シングルクォート対策）
# =========================
def _concat_escape(path: Path) -> str:
    # ffmpeg concat demuxer の file '...'
    # → シングルクォートが入ると壊れるのでエスケープ
    s = str(path)
    return s.replace("'", r"'\''")


def make_concat_list(images: List[Path], durations: List[float], out_txt: Path) -> None:
    if len(images) != len(durations):
        die("images と durations の長さが一致しません。")
    if not images:
        die("concat list: images が0件です。")

    lines: List[str] = []
    for img, d in zip(images, durations):
        dd = max(float(MIN_SEG_SEC), float(d))
        lines.append(f"file '{_concat_escape(img)}'")
        lines.append(f"duration {dd:.6f}")

    # concat demuxer仕様：最後にもう一回 file を書く
    lines.append(f"file '{_concat_escape(images[-1])}'")

    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =========================
# 画像/音声素材収集
# =========================
def _num_key_from_stem(p: Path) -> Tuple[int, str]:
    # 先頭数字があれば数字ソート、それ以外は文字列
    m = re.match(r"^(\d+)", p.stem)
    if m:
        return (int(m.group(1)), p.name)
    return (10**12, p.name)


def collect_main_images(main_dir_candidates: List[Path], main_single_candidates: List[Path]) -> List[Path]:
    for d in main_dir_candidates:
        if d.exists():
            imgs = [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS]
            if imgs:
                # 1,2,10 みたいな並びを自然に
                imgs_sorted = sorted(imgs, key=_num_key_from_stem)
                return imgs_sorted

    for p in main_single_candidates:
        if p.exists():
            return [p]

    die(f"メイン画像が見つかりません: {main_dir_candidates} / {main_single_candidates}")
    return []


def allocate_equal_durations(total: float, n: int) -> List[float]:
    if n <= 0:
        return []
    per = max(float(MIN_SEG_SEC), total / n)
    ds = [per] * n
    # 合計をtotalに近づける（最後で吸収）
    ds[-1] = max(float(MIN_SEG_SEC), total - sum(ds[:-1]))
    return ds


def collect_comment_ranks(comment_dir: Path) -> List[int]:
    if not comment_dir.exists():
        die(f"commentフォルダが見つかりません: {comment_dir}")
    ranks: List[int] = []
    for p in comment_dir.iterdir():
        if not p.is_file() or p.suffix.lower() not in IMAGE_EXTS:
            continue
        m = IMG_RE.match(p.name)
        if m:
            ranks.append(int(m.group(1)))
    if not ranks:
        die(f"コメント画像が見つかりません（例: 15.png）: {comment_dir}")
    return sorted(set(ranks), reverse=True)


def build_voice_map(voice_dir: Path) -> Dict[int, Tuple[int, Path]]:
    if not voice_dir.exists():
        die(f"voiceフォルダが見つかりません: {voice_dir}")
    mp: Dict[int, Tuple[int, Path]] = {}
    for p in voice_dir.iterdir():
        if not p.is_file():
            continue
        if p.name.lower() == "all.wav":
            continue
        m = SEG_RE.match(p.name)
        if not m:
            continue
        idx = int(m.group(1))
        ms = int(m.group(2))
        if ms <= 0:
            continue
        mp[idx] = (ms, p)
    if not mp:
        die(f"分割音声が見つかりません（例: 15_4000ms.wav）: {voice_dir}")
    return mp


def concat_wavs(paths: List[Path], out_path: Path) -> None:
    if not paths:
        die("連結対象のwavが0件です。")
    params0 = None
    frames_all: List[bytes] = []
    for p in paths:
        with wave.open(str(p), "rb") as wf:
            params = wf.getparams()
            if params0 is None:
                params0 = params
            else:
                if (params.nchannels, params.sampwidth, params.framerate) != (params0.nchannels, params0.sampwidth, params0.framerate):
                    die(f"wav形式が一致しません: {p} / {params} vs {params0}")
            frames_all.append(wf.readframes(wf.getnframes()))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(out_path), "wb") as out:
        assert params0 is not None
        out.setnchannels(params0.nchannels)
        out.setsampwidth(params0.sampwidth)
        out.setframerate(params0.framerate)
        for fr in frames_all:
            out.writeframes(fr)


def safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        src.replace(dst)
    except Exception:
        shutil.copy2(src, dst)


# =========================
# duration調整（ズレても落ちにくくする）
# =========================
def fit_durations_to_total(durs: List[float], total: float, *, min_sec: float) -> List[float]:
    """
    durs合計を total に合わせる。
    - total より長すぎる場合は全体を比率で縮める
    - どれも min_sec を下回らない
    - 最後で丸め誤差を吸収
    """
    if not durs:
        return []
    min_sec = max(0.0, float(min_sec))
    total = float(total)

    # 下限適用
    d = [max(min_sec, float(x)) for x in durs]

    s = sum(d)
    if total <= 0:
        return d

    if s > total:
        # 全体を縮める（ただし下限あり）
        scale = total / s
        d2 = [max(min_sec, x * scale) for x in d]
        # まだ超えるなら、下限が効きすぎているので最後だけ切る
        s2 = sum(d2)
        if s2 > total:
            # 可能な範囲で最後を削る（0にならないよう min_sec）
            over = s2 - total
            d2[-1] = max(min_sec, d2[-1] - over)
        d = d2
    else:
        # 足りない分は最後に足す
        d[-1] = max(min_sec, d[-1] + (total - s))

    # 最後に微調整（浮動小数の誤差吸収）
    s3 = sum(d)
    if abs(s3 - total) > 1e-3:
        d[-1] = max(min_sec, d[-1] + (total - s3))

    return d


# =========================
# filter_complex 生成
# =========================
def _build_fc_video(T: float) -> str:
    return f"""
color=c=black:s={W}x{H}:r={FPS}:d={T:.6f}[base];
[0:v]setpts=PTS-STARTPTS,
scale={SIZE_MAIN[0]}:{SIZE_MAIN[1]}:force_original_aspect_ratio=decrease,
pad={SIZE_MAIN[0]}:{SIZE_MAIN[1]}:(ow-iw)/2:(oh-ih)/2:color=0x00000000,
format=rgba[main];
[1:v]setpts=PTS-STARTPTS,
scale={SIZE_COMMENT[0]}:{SIZE_COMMENT[1]}:force_original_aspect_ratio=decrease,
pad={SIZE_COMMENT[0]}:{SIZE_COMMENT[1]}:(ow-iw)/2:(oh-ih)/2:color=0x00000000,
format=rgba[com];
[2:v]setpts=PTS-STARTPTS,
scale={SIZE_TITLE[0]}:{SIZE_TITLE[1]}:force_original_aspect_ratio=decrease,
pad={SIZE_TITLE[0]}:{SIZE_TITLE[1]}:(ow-iw)/2:(oh-ih)/2:color=0x00000000,
format=rgba[tit];
[base][tit]overlay=0:{Y_TITLE[0]}:format=auto[tmp1];
[tmp1][main]overlay=0:{Y_MAIN[0]}:format=auto[tmp2];
[tmp2][com]overlay=0:{Y_COMMENT[0]}:format=auto,fps={FPS},format=yuv420p[v];
""".strip()


def _build_fc_audio(T: float, bgm_path: Optional[Path], bgm_start: float, ducking: bool) -> str:
    if not bgm_path:
        return f"""
[3:a]aresample=48000,atrim=0:{T:.6f},asetpts=PTS-STARTPTS[a]
""".strip()

    fade_out_start = max(0.0, float(T) - float(BGM_FADE_SEC))
    bgm_end = bgm_start + float(T)

    if ducking:
        return f"""
[3:a]aresample=48000,atrim=0:{T:.6f},asetpts=PTS-STARTPTS[voice];
[4:a]aresample=48000,atrim=start={bgm_start:.6f}:end={bgm_end:.6f},asetpts=PTS-STARTPTS,
volume={BGM_VOLUME},
afade=t=in:st=0:d={float(BGM_FADE_SEC):.6f},
afade=t=out:st={fade_out_start:.6f}:d={float(BGM_FADE_SEC):.6f}[bgm];
[bgm][voice]sidechaincompress=threshold=0.03:ratio=10:attack=5:release=200[bgmduck];
[voice][bgmduck]amix=inputs=2:duration=first:dropout_transition=2[a]
""".strip()

    return f"""
[3:a]aresample=48000,atrim=0:{T:.6f},asetpts=PTS-STARTPTS[voice];
[4:a]aresample=48000,atrim=start={bgm_start:.6f}:end={bgm_end:.6f},asetpts=PTS-STARTPTS,
volume={BGM_VOLUME},
afade=t=in:st=0:d={float(BGM_FADE_SEC):.6f},
afade=t=out:st={fade_out_start:.6f}:d={float(BGM_FADE_SEC):.6f}[bgm];
[voice][bgm]amix=inputs=2:duration=first:dropout_transition=2[a]
""".strip()


# =========================
# 締め動画（静止画 + last.wav）生成
# =========================
def build_ending_video(ending_img: Path, ending_wav: Path, out_mp4: Path, log_path: Path) -> float:
    if not ending_img.exists():
        die(f"ending image not found: {ending_img}")
    if not ending_wav.exists():
        die(f"ending audio not found: {ending_wav}")

    T = audio_duration_sec_wav(ending_wav) + float(ENDING_PAD_SEC)
    if T <= 0:
        die("ending audio duration is 0s")

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(ending_img),
        "-i", str(ending_wav),
        "-t", f"{T:.6f}",
        "-vf", f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
               f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:black,"
               f"fps={FPS},format=yuv420p",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-ar", "48000",
        "-ac", "2",
    ]
    if ENABLE_FASTSTART:
        cmd += ["-movflags", "+faststart"]
    cmd += [str(out_mp4)]
    run(cmd, log_path=log_path)
    return T


def concat_two_videos_reencode(a_mp4: Path, b_mp4: Path, out_mp4: Path, log_path: Path) -> None:
    fc = "[0:v][0:a][1:v][1:a]concat=n=2:v=1:a=1[v][a]"
    cmd = [
        "ffmpeg", "-y",
        "-i", str(a_mp4),
        "-i", str(b_mp4),
        "-filter_complex", fc,
        "-map", "[v]",
        "-map", "[a]",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-ar", "48000",
        "-ac", "2",
    ]
    if ENABLE_FASTSTART:
        cmd += ["-movflags", "+faststart"]
    cmd += [str(out_mp4)]
    run(cmd, log_path=log_path)


# =========================
# ビルド本体
# =========================
def run_build(parent_dir: Path, item_id: int) -> Path:
    ensure_tools()

    base_dir = parent_dir
    title_img = base_dir / "image" / "title" / "title.png"
    comment_dir = base_dir / "image" / "comment"
    voice_dir = base_dir / "voice"
    voice_all = voice_dir / "all.wav"

    preview_mp4 = base_dir / PREVIEW_REL

    main_dir_candidates = [base_dir / "image" / "main", base_dir / "image" / "Main"]
    main_single_candidates = [
        base_dir / "image" / "main_image.jpeg",
        base_dir / "image" / "Main" / "1.jpeg",
        base_dir / "image" / "Main" / "1.jpg",
        base_dir / "image" / "Main" / "1.png",
        base_dir / "image" / "main" / "1.jpeg",
        base_dir / "image" / "main" / "1.jpg",
        base_dir / "image" / "main" / "1.png",
    ]

    movie_dir = base_dir / "movie"
    out_mp4 = movie_dir / "youtube_upload.mp4"

    log_dir = base_dir / "_logs"
    ffmpeg_log = log_dir / "ffmpeg_build.log"
    ffmpeg_log_noduck = log_dir / "ffmpeg_build_noduck.log"
    ffmpeg_log_ending = log_dir / "ffmpeg_ending.log"
    ffmpeg_log_concat_ending = log_dir / "ffmpeg_concat_ending.log"
    ffmpeg_log_concat_preview = log_dir / "ffmpeg_concat_preview.log"
    ffmpeg_log_fix_audio_a = log_dir / "ffmpeg_fix_audio_a.log"
    ffmpeg_log_fix_audio_b = log_dir / "ffmpeg_fix_audio_b.log"

    tmp_video_main = TMP_DIR / f"youtube_upload_{item_id}_tmp_main.mp4"
    tmp_video_body = TMP_DIR / f"youtube_upload_{item_id}_tmp_body.mp4"
    tmp_video_final = TMP_DIR / f"youtube_upload_{item_id}_tmp_final.mp4"
    tmp_ending_video = TMP_DIR / f"youtube_upload_{item_id}_tmp_ending.mp4"
    tmp_audio_desc = TMP_DIR / f"all_desc_{item_id}.wav"
    tmp_preview_fixed = TMP_DIR / f"preview_fixed_{item_id}.mp4"
    tmp_body_fixed = TMP_DIR / f"body_fixed_{item_id}.mp4"

    # cleanup対象
    tmp_cleanup: List[Path] = [
        tmp_video_main, tmp_video_body, tmp_video_final, tmp_ending_video,
        tmp_audio_desc, tmp_preview_fixed, tmp_body_fixed,
    ]

    if not base_dir.exists():
        die(f"対象フォルダが見つかりません: {base_dir}")
    if not title_img.exists():
        die(f"タイトル画像が見つかりません: {title_img}")
    if not voice_all.exists():
        die(f"all.wav が見つかりません: {voice_all}")

    # preview 判定（未定義参照が起きない形に整理）
    local_enable_preview = False
    if ENABLE_PREVIEW:
        if preview_mp4.exists():
            local_enable_preview = True
            print(f"[INFO] preview enabled: {preview_mp4}")
        else:
            msg = f"preview.mp4 not found: {preview_mp4}"
            if PREVIEW_REQUIRED:
                die(msg)
            print(f"[WARN] {msg} -> skip preview")

    if ENABLE_ENDING:
        if not ENDING_IMAGE_PATH.exists():
            die(f"締め画像が見つかりません: {ENDING_IMAGE_PATH}")
        if not ENDING_AUDIO_PATH.exists():
            die(f"締め音声が見つかりません: {ENDING_AUDIO_PATH}")

    movie_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ranks / voice_map
        ranks = collect_comment_ranks(comment_dir)
        print("[INFO] ranks (desc):", ranks[:10], "..." if len(ranks) > 10 else "")

        voice_map = build_voice_map(voice_dir)

        comment_imgs: List[Path] = []
        comment_durs_raw: List[float] = []
        seg_paths_desc: List[Path] = []

        for r in ranks:
            img = None
            for ext in (".png", ".jpg", ".jpeg", ".webp"):
                p = comment_dir / f"{r}{ext}"
                if p.exists():
                    img = p
                    break
            if img is None:
                die(f"コメント画像がありません: {comment_dir}/{r}.(png/jpg/jpeg/webp)")

            if r not in voice_map:
                die(f"対応する分割音声がありません: voice/{r}_xxxxms.wav が必要です。")

            ms, seg_path = voice_map[r]
            comment_imgs.append(img)
            comment_durs_raw.append(ms / 1000.0)
            seg_paths_desc.append(seg_path)

        # audio_for_video
        audio_for_video = voice_all
        if AUTO_BUILD_AUDIO_DESC:
            concat_wavs(seg_paths_desc, tmp_audio_desc)
            audio_for_video = tmp_audio_desc
            print(f"[INFO] built desc audio: {audio_for_video}")

        T = audio_duration_sec_wav(audio_for_video)
        if T <= 0:
            die("音声の長さが0秒です。")
        print(f"[INFO] audio duration: {T:.3f}s")

        # durations をTにフィット（ズレても落ちにくくする）
        comment_durs = fit_durations_to_total(comment_durs_raw, T, min_sec=float(MIN_SEG_SEC))

        bgm_path, bgm_start = pick_bgm_by_id(item_id)
        if bgm_path:
            parity = "odd" if (item_id % 2 == 1) else "even"
            print(f"[INFO] BGM selected: id={item_id} ({parity}) -> {bgm_path.name} (start={bgm_start:.3f}s)")
        else:
            print(f"[INFO] BGM: (none) id={item_id}")

        # main images
        main_imgs = collect_main_images(main_dir_candidates, main_single_candidates)
        main_durs = allocate_equal_durations(T, len(main_imgs))

        # concat lists
        tmp = base_dir / "_build_tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        main_list = tmp / "main_concat.txt"
        comment_list = tmp / "comment_concat.txt"
        make_concat_list(main_imgs, main_durs, main_list)
        make_concat_list(comment_imgs, comment_durs, comment_list)

        ffmpeg_out_main = tmp_video_main if WRITE_TO_LOCAL_TMP else (movie_dir / "youtube_upload_body_main.mp4")

        def build_cmd_main(ducking: bool) -> List[str]:
            fc_video = _build_fc_video(T)
            fc_audio = _build_fc_audio(T, bgm_path, bgm_start, ducking=ducking)
            fc = (fc_video + "\n" + fc_audio).strip()

            cmd: List[str] = [
                "ffmpeg", "-y",
                "-f", "concat", "-safe", "0", "-i", str(main_list),
                "-f", "concat", "-safe", "0", "-i", str(comment_list),
                "-loop", "1", "-i", str(title_img),
                "-i", str(audio_for_video),
            ]
            if bgm_path:
                cmd += ["-stream_loop", "-1", "-i", str(bgm_path)]

            cmd += [
                "-filter_complex", fc,
                "-map", "[v]",
                "-map", "[a]",
                "-t", f"{T:.6f}",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ar", "48000",
                "-ac", "2",
            ]
            if ENABLE_FASTSTART:
                cmd += ["-movflags", "+faststart"]
            cmd += [str(ffmpeg_out_main)]
            return cmd

        # 本編作成（ダッキング→失敗時ノーダック）
        try:
            run(build_cmd_main(ducking=bool(BGM_DUCKING)), log_path=ffmpeg_log)
        except Exception:
            if bgm_path and BGM_DUCKING:
                print("[WARN] ffmpeg failed with ducking. Retrying without ducking...")
                run(build_cmd_main(ducking=False), log_path=ffmpeg_log_noduck)
            else:
                raise

        # ending を付けた「ボディ」を作る
        body_mp4 = tmp_video_body if WRITE_TO_LOCAL_TMP else (movie_dir / "youtube_upload_body.mp4")
        if ENABLE_ENDING:
            print(f"[INFO] append ending: image={ENDING_IMAGE_PATH.name} audio={ENDING_AUDIO_PATH.name}")
            build_ending_video(
                ending_img=ENDING_IMAGE_PATH,
                ending_wav=ENDING_AUDIO_PATH,
                out_mp4=tmp_ending_video,
                log_path=ffmpeg_log_ending,
            )
            concat_two_videos_reencode(
                a_mp4=ffmpeg_out_main,
                b_mp4=tmp_ending_video,
                out_mp4=body_mp4,
                log_path=ffmpeg_log_concat_ending,
            )
        else:
            if WRITE_TO_LOCAL_TMP:
                shutil.copy2(ffmpeg_out_main, body_mp4)
            else:
                body_mp4 = ffmpeg_out_main

        # preview を冒頭に付けて最終化（音声無しでも落とさない）
        final_mp4 = tmp_video_final if WRITE_TO_LOCAL_TMP else out_mp4

        a_for_concat = preview_mp4
        b_for_concat = body_mp4

        if local_enable_preview:
            if ENSURE_AUDIO_FOR_CONCAT and shutil.which("ffprobe") is not None:
                a_for_concat = ensure_audio_track(preview_mp4, tmp_preview_fixed, log_path=ffmpeg_log_fix_audio_a)
                b_for_concat = ensure_audio_track(body_mp4, tmp_body_fixed, log_path=ffmpeg_log_fix_audio_b)

            print(f"[INFO] prepend preview: {a_for_concat}")
            concat_two_videos_reencode(
                a_mp4=a_for_concat,
                b_mp4=b_for_concat,
                out_mp4=final_mp4,
                log_path=ffmpeg_log_concat_preview,
            )
        else:
            if WRITE_TO_LOCAL_TMP:
                shutil.copy2(body_mp4, final_mp4)
            else:
                final_mp4 = body_mp4

        if WRITE_TO_LOCAL_TMP:
            safe_copy(final_mp4, out_mp4)

        print("=== DONE ===")
        print(f"OUTPUT: {out_mp4}")
        return out_mp4

    finally:
        if CLEANUP_TMP:
            for p in tmp_cleanup:
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass


# =========================
# メイン（DBキューで1件拾って処理→check_create更新）
# =========================
def main() -> int:
    print(f"[INFO] now={now_jst()}")
    print(f"[INFO] env: {_ENV_PATH}")
    print(f"[INFO] DB_PATH={DB_PATH}")
    print(f"[INFO] BASE_OUTPUT_ROOT={BASE_OUTPUT_ROOT}")
    print(f"[INFO] stage: STA_99={STA_99} -> END_99={END_99}")
    print(f"[INFO] sqlite: journal_mode={SQLITE_JOURNAL_MODE} synchronous={SQLITE_SYNCHRONOUS} busy_timeout_ms={BUSY_TIMEOUT_MS}")
    print(f"[INFO] MIN_SEG_SEC={MIN_SEG_SEC} CLEANUP_TMP={CLEANUP_TMP}")
    print(f"[INFO] ENSURE_AUDIO_FOR_CONCAT={ENSURE_AUDIO_FOR_CONCAT}")

    try:
        con = connect_db(DB_PATH)
    except Exception as e:
        print(f"[ERROR] DB open failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    try:
        ensure_columns(con)

        row = pick_one(con)
        if row is None:
            print(f"[INFO] no item with check_create={STA_99}.")
            return 0

        item_id = int(row["id"])
        folder_name = str(row["folder_name"])
        parent_dir = BASE_OUTPUT_ROOT / folder_name

        print(f"[INFO] picked id={item_id} folder_name={folder_name}")
        print(f"[INFO] parent_dir={parent_dir}")

        # 99単体多重起動対策：軽くclaimする
        if not claim_job_atomic(con, item_id=item_id):
            print(f"[INFO] claim failed (maybe taken by another process). id={item_id}")
            return 0

        try:
            if not parent_dir.exists():
                raise FileNotFoundError(f"parent_dir not found: {parent_dir}")

            out_mp4 = run_build(parent_dir, item_id=item_id)
            print(f"[OK] created: {out_mp4}")

            update_stage_success(con, item_id=item_id)
            print(f"[OK] done. check_create {STA_99} -> {END_99} (id={item_id})")
            return 0

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            update_stage_error(con, item_id=item_id, err=err)
            print(f"[ERROR] failed id={item_id} kept check_create={STA_99}. {err}", file=sys.stderr)
            return 1

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
