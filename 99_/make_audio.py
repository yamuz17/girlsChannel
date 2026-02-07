#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import json
import re
import wave
import sys
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

import queue_db


# =========================
# env load
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
queue_db.load_env()

CFG = queue_db.build_queue_config_from_env()
BASE_OUTPUT_ROOT = Path(queue_db._env_str("BASE_OUTPUT_ROOT", "")).expanduser()

PICK_ORDER = queue_db._env_str("PICK_ORDER", "post_date_desc").strip() or "post_date_desc"
STA_04 = queue_db._env_int("STA_04", 3)
END_04 = queue_db._env_int("END_04", 4)

ENGINE_URL = queue_db._env_str("ENGINE_URL", "http://127.0.0.1:50021").strip()

VOICE_APP_CANDIDATES = [p.strip() for p in queue_db._env_str("VOICE_APP_CANDIDATES", "").split(";") if p.strip()] or [
    "/Applications/VOICEVOX.app",
    "/Applications/VOICEBOX.app",
]
VOICE_BOOT_TIMEOUT_SEC = float(queue_db._env_str("VOICE_BOOT_TIMEOUT_SEC", "60"))
VOICE_POLL_INTERVAL_SEC = float(queue_db._env_str("VOICE_POLL_INTERVAL_SEC", "2"))

TOTAL_VIDEO_SEC = float(queue_db._env_str("TOTAL_VIDEO_SEC", "45.0"))
SILENCE_MS = int(queue_db._env_int("SILENCE_MS", 400))
MIN_SEC_PER_COMMENT = float(queue_db._env_str("MIN_SEC_PER_COMMENT", "2.0"))
MAX_SEC_PER_COMMENT = float(queue_db._env_str("MAX_SEC_PER_COMMENT", "7.5"))

SPEED_MIN = float(queue_db._env_str("SPEED_MIN", "0.75"))
SPEED_MAX = float(queue_db._env_str("SPEED_MAX", "1.60"))
TOL_MS = int(queue_db._env_int("TOL_MS", 40))

REMOVE_EMOJI = queue_db._env_bool("REMOVE_EMOJI", True)
COMPRESS_LONG_BAR = int(queue_db._env_int("COMPRESS_LONG_BAR", 3))

VOICE_CYCLE = [
    ("四国めたん", "ノーマル"),
    ("春日部つむぎ", "ノーマル"),
    ("ずんだもん", "ノーマル"),
]

VOICE_DIRNAME = "voice"
TEXT_DIRNAME = "text"
ALL_WAV_NAME = "all.wav"


# =========================
# VOICE ready
# =========================
def _is_engine_ready(engine_url: str) -> bool:
    try:
        r = requests.get(f"{engine_url}/speakers", timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _launch_voice_app() -> None:
    for app in VOICE_APP_CANDIDATES:
        if Path(app).exists():
            subprocess.Popen(["open", app])
            return
    subprocess.Popen(["open", "-a", "VOICEVOX"])


def ensure_voice_engine_ready(engine_url: str) -> None:
    if _is_engine_ready(engine_url):
        print("[INFO] VOICE engine is ready.")
        return

    print(f"[INFO] VOICE engine not ready: {engine_url}")
    print("[INFO] VOICE app is not running -> launching...")
    _launch_voice_app()

    start = time.time()
    while True:
        if _is_engine_ready(engine_url):
            print("[INFO] VOICE engine became ready.")
            return

        elapsed = time.time() - start
        remain = int(VOICE_BOOT_TIMEOUT_SEC - elapsed)
        if remain <= 0:
            raise RuntimeError(f"VOICE engine not ready after {int(VOICE_BOOT_TIMEOUT_SEC)}s: {engine_url}")

        print(f"[INFO] wait for VOICE engine... ({remain}s left)")
        time.sleep(VOICE_POLL_INTERVAL_SEC)


# =========================
# body
# =========================
_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "]+",
    flags=re.UNICODE,
)


def clean_text(s: str) -> str:
    s = (s or "").strip()
    if REMOVE_EMOJI:
        s = _EMOJI_RE.sub("", s)
    if COMPRESS_LONG_BAR and COMPRESS_LONG_BAR > 0:
        s = re.sub(r"ー{" + str(COMPRESS_LONG_BAR + 1) + r",}", "ー" * COMPRESS_LONG_BAR, s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def iter_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def find_ratio_ndjson(text_dir: Path) -> Path:
    ratio_files = sorted([p for p in text_dir.glob("*.ndjson") if "ratio" in p.name.lower()])
    if not ratio_files:
        raise FileNotFoundError(f"ratio系NDJSONが見つかりません: {text_dir}/*.ndjson（ファイル名にratioが必要）")
    return ratio_files[0]


def fetch_speakers() -> list[dict]:
    r = requests.get(f"{ENGINE_URL}/speakers", timeout=30)
    r.raise_for_status()
    return r.json()


def find_style_id(speakers: list[dict], speaker_name: str, style_name: str = "ノーマル") -> int:
    for sp in speakers:
        name = sp.get("name", "")
        if speaker_name in name:
            styles = sp.get("styles", [])
            for st in styles:
                if style_name in st.get("name", ""):
                    return int(st["id"])
            if styles:
                return int(styles[0]["id"])
    raise ValueError(f"話者が見つかりません: {speaker_name}")


def wav_duration_ms(wav_bytes: bytes) -> int:
    with wave.open(io.BytesIO(wav_bytes), "rb") as w:
        frames = w.getnframes()
        rate = w.getframerate()
    return int(frames * 1000 / rate)


def synth_voicevox(text: str, style_id: int, *, speed_scale: float | None = None) -> bytes:
    q = requests.post(f"{ENGINE_URL}/audio_query", params={"text": text, "speaker": style_id}, timeout=30)
    q.raise_for_status()
    query = q.json()
    if speed_scale is not None:
        query["speedScale"] = float(speed_scale)

    s = requests.post(f"{ENGINE_URL}/synthesis", params={"speaker": style_id}, json=query, timeout=120)
    s.raise_for_status()
    return s.content


def force_wav_to_target_ms(wav_bytes: bytes, target_ms: int) -> bytes:
    with wave.open(io.BytesIO(wav_bytes), "rb") as w:
        params = w.getparams()
        frames = w.readframes(w.getnframes())

    nch = params.nchannels
    sw = params.sampwidth
    fr = params.framerate
    frame_size = nch * sw

    cur_frames = len(frames) // frame_size
    cur_ms = int(cur_frames * 1000 / fr)

    if cur_ms < target_ms:
        need_frames = int((target_ms - cur_ms) * fr / 1000)
        frames += b"\x00" * (need_frames * frame_size)
    elif cur_ms > target_ms:
        keep_frames = int(target_ms * fr / 1000)
        frames = frames[: keep_frames * frame_size]

    out = io.BytesIO()
    with wave.open(out, "wb") as wo:
        wo.setnchannels(nch)
        wo.setsampwidth(sw)
        wo.setframerate(fr)
        wo.writeframes(frames)
    return out.getvalue()


def ensure_duration_by_speedscale(text: str, style_id: int, target_ms: int) -> bytes:
    wav1 = synth_voicevox(text, style_id, speed_scale=None)
    d1 = wav_duration_ms(wav1)

    if abs(d1 - target_ms) <= TOL_MS:
        return force_wav_to_target_ms(wav1, target_ms)

    scale = d1 / target_ms
    speed = max(SPEED_MIN, min(SPEED_MAX, scale))

    wav2 = synth_voicevox(text, style_id, speed_scale=speed)
    return force_wav_to_target_ms(wav2, target_ms)


def allocate_targets_ms_by_chars(
    texts: List[str],
    total_video_ms: int,
    silence_ms: int,
    min_ms: int,
    max_ms: int,
) -> List[int]:
    n = len(texts)
    available_ms = total_video_ms - silence_ms * (n - 1)
    if available_ms <= 0:
        raise ValueError("TOTAL_VIDEO_SEC が短すぎて、無音分だけで埋まっています。")

    weights = [max(len(t), 1) for t in texts]
    wsum = sum(weights)

    raw = [int(round(available_ms * w / wsum)) for w in weights]
    targets = [min(max(x, min_ms), max_ms) for x in raw]

    def total(x): return sum(x)

    for _ in range(10):
        cur = total(targets)
        diff = available_ms - cur
        if abs(diff) <= 5:
            break

        if diff > 0:
            candidates = [i for i in range(n) if targets[i] < max_ms]
            if not candidates:
                break
            c_wsum = sum(weights[i] for i in candidates)
            for i in candidates:
                add = int(round(diff * (weights[i] / c_wsum)))
                if add > 0:
                    targets[i] = min(max_ms, targets[i] + add)
        else:
            diff = -diff
            candidates = [i for i in range(n) if targets[i] > min_ms]
            if not candidates:
                break
            c_wsum = sum(weights[i] for i in candidates)
            for i in candidates:
                sub = int(round(diff * (weights[i] / c_wsum)))
                if sub > 0:
                    targets[i] = max(min_ms, targets[i] - sub)

    return targets


def concat_wavs_with_silence(wav_paths: List[Path], out_wav: Path, silence_ms: int):
    if not wav_paths:
        raise ValueError("wav_paths is empty")

    with wave.open(str(wav_paths[0]), "rb") as w0:
        nchannels = w0.getnchannels()
        sampwidth = w0.getsampwidth()
        framerate = w0.getframerate()

    frame_size = nchannels * sampwidth
    silence_frames = int(framerate * (silence_ms / 1000.0))
    silence_bytes = b"\x00" * (silence_frames * frame_size)

    out_wav.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(out_wav), "wb") as wo:
        wo.setnchannels(nchannels)
        wo.setsampwidth(sampwidth)
        wo.setframerate(framerate)

        for i, wf in enumerate(wav_paths):
            with wave.open(str(wf), "rb") as wi:
                if (wi.getnchannels(), wi.getsampwidth(), wi.getframerate()) != (nchannels, sampwidth, framerate):
                    raise RuntimeError(f"WAV形式が揃っていません: {wf.name}")
                wo.writeframes(wi.readframes(wi.getnframes()))
            if i != len(wav_paths) - 1 and silence_ms > 0:
                wo.writeframes(silence_bytes)


def run_voice_job(parent_dir: Path) -> None:
    text_dir = parent_dir / TEXT_DIRNAME
    voice_dir = parent_dir / VOICE_DIRNAME
    voice_dir.mkdir(parents=True, exist_ok=True)

    ndjson_path = find_ratio_ndjson(text_dir)

    meta = None
    items: List[Tuple[int, str]] = []
    for obj in iter_ndjson(ndjson_path):
        if "meta" in obj and meta is None:
            meta = obj["meta"]
            continue
        if "text" in obj:
            rank = obj.get("rank")
            text = clean_text(obj.get("text", ""))
            if isinstance(rank, int) and text:
                items.append((rank, text))

    if not items:
        raise RuntimeError("読み上げ対象がありません（rank/textが空など）")

    items.sort(key=lambda x: x[0], reverse=True)

    speakers = fetch_speakers()
    cycle_ids = [find_style_id(speakers, sp, st) for sp, st in VOICE_CYCLE]

    total_ms = int(TOTAL_VIDEO_SEC * 1000)
    min_ms = int(MIN_SEC_PER_COMMENT * 1000)
    max_ms = int(MAX_SEC_PER_COMMENT * 1000)

    texts = [t for _, t in items]
    targets_ms = allocate_targets_ms_by_chars(texts, total_ms, SILENCE_MS, min_ms, max_ms)

    print("PARENT_DIR:", parent_dir)
    print("ndjson:", ndjson_path)
    print("voice_dir:", voice_dir)
    print("targets(sec):", [round(t / 1000, 2) for t in targets_ms])

    per_paths_in_order: List[Path] = []

    for order_idx, ((rank, text), target_ms) in enumerate(zip(items, targets_ms), start=1):
        style_id = cycle_ids[(order_idx - 1) % len(cycle_ids)]
        wav_fixed = ensure_duration_by_speedscale(text, style_id, target_ms)

        out_path = voice_dir / f"{rank}_{target_ms}ms.wav"
        out_path.write_bytes(wav_fixed)
        per_paths_in_order.append(out_path)

        print(f"saved: {out_path.name} (rank={rank}, target={target_ms}ms)")

    all_path = voice_dir / ALL_WAV_NAME
    concat_wavs_with_silence(per_paths_in_order, all_path, SILENCE_MS)
    print("all ->", all_path)


def main() -> int:
    print(f"[INFO] {queue_db.now_jst()}")
    print(f"[INFO] DB: {CFG.db_path}")
    print(f"[INFO] table={CFG.table} STA_04={STA_04} END_04={END_04} order={PICK_ORDER}")
    print(f"[INFO] BASE_OUTPUT_ROOT: {BASE_OUTPUT_ROOT}")

    try:
        with queue_db.connect_db(CFG) as con:
            queue_db.ensure_common_columns(con, CFG.table)

            picked = queue_db.pick_one(con, CFG.table, STA_04, PICK_ORDER)
            if picked is None:
                print(f"[INFO] no item with check_create={STA_04}.")
                return 0

            item_id, folder_name = picked
            parent_dir = BASE_OUTPUT_ROOT / folder_name

            print(f"[INFO] picked id={item_id} folder_name={folder_name}")
            print(f"[INFO] parent_dir={parent_dir}")

            try:
                if not parent_dir.exists():
                    raise FileNotFoundError(f"parent_dir not found: {parent_dir}")

                ensure_voice_engine_ready(ENGINE_URL)
                run_voice_job(parent_dir)

                queue_db.mark_done(con, CFG.table, item_id, STA_04, END_04)
                print(f"[OK] done. check_create {STA_04} -> {END_04} (id={item_id})")
                return 0

            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                queue_db.mark_fail(con, CFG.table, item_id, STA_04, err)
                print(f"[ERROR] failed id={item_id} kept check_create={STA_04}. {err}", file=sys.stderr)
                return 1

    except Exception as e:
        print(f"[ERROR] DB operation failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
