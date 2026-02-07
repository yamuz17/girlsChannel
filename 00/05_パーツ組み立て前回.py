#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import wave
import sqlite3
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

# =========================
# 設定（ここだけ変えればOK）
# =========================
from config import DB_PATH
TABLE_NAME = "items"
BASE_OUTPUT_ROOT = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")

PICK_STATUS = 4
DONE_STATUS = 5
PICK_ORDER = "post_date_desc"   # "post_date_desc" or "comments_desc"

FPS = 30
AUTO_BUILD_AUDIO_DESC = True
WRITE_TO_LOCAL_TMP = True
ENABLE_FASTSTART = False

# === BGM（奇数/偶数で 1.mp3 / 2.mp3 を出し分け）===
ENABLE_BGM = True
BGM_ODD_PATH  = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/bgm/1.mp3")
BGM_EVEN_PATH = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/bgm/2.mp3")

BGM_VOLUME = 0.12
BGM_DUCKING = True
BGM_FADE_SEC = 0.30

# （任意）BGMの開始位置（秒）
BGM_START_SEC_ODD = 0.0
BGM_START_SEC_EVEN = 0.0

# =========================
# 固定
# =========================
TMP_DIR = Path("/tmp")
W, H = 1080, 1920

Y_TITLE   = (0, 384)
Y_MAIN    = (384, 960)
Y_COMMENT = (960, 1728)

SIZE_TITLE   = (W, Y_TITLE[1]   - Y_TITLE[0])     # 1080x384
SIZE_MAIN    = (W, Y_MAIN[1]    - Y_MAIN[0])      # 1080x576
SIZE_COMMENT = (W, Y_COMMENT[1] - Y_COMMENT[0])   # 1080x768

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
SEG_RE = re.compile(r"^(\d+)_(\d+)ms\.wav$", re.IGNORECASE)
IMG_RE = re.compile(r"^(\d+)\.(png|jpg|jpeg|webp)$", re.IGNORECASE)

# =========================
# DBユーティリティ
# =========================
def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con

def _now_jst_str() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")

def _ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.execute(f"PRAGMA table_info({TABLE_NAME})")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(sql: str):
        try:
            con.execute(sql)
        except Exception:
            pass

    if "check_create" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN check_create INTEGER DEFAULT 0")
    if "folder_name" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN folder_name TEXT")
    if "last_error" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN last_error TEXT")
    if "updated_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN updated_at TEXT")

    if "video_created" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created INTEGER DEFAULT 0")
    if "video_created_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created_at TEXT")
    if "video_uploaded" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded INTEGER DEFAULT 0")
    if "video_uploaded_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded_at TEXT")

    con.commit()

def _pick_one(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    order_sql = "id DESC"
    if PICK_ORDER == "post_date_desc":
        order_sql = "post_date DESC, id DESC"
    elif PICK_ORDER == "comments_desc":
        order_sql = "comment_count DESC, id DESC"

    sql = f"""
        SELECT *
          FROM {TABLE_NAME}
         WHERE check_create = ?
           AND folder_name IS NOT NULL
           AND folder_name != ''
         ORDER BY {order_sql}
         LIMIT 1
    """
    return con.execute(sql, (PICK_STATUS,)).fetchone()

def _update_status(con: sqlite3.Connection, item_id: int, status: int, last_error: Optional[str]) -> None:
    con.execute(
        f"""
        UPDATE {TABLE_NAME}
           SET check_create = ?,
               last_error   = ?,
               updated_at   = ?
         WHERE id = ?
        """,
        (status, last_error, _now_jst_str(), item_id),
    )
    con.commit()

def _mark_video_created(con: sqlite3.Connection, item_id: int) -> None:
    con.execute(
        f"""
        UPDATE {TABLE_NAME}
           SET video_created    = 1,
               video_created_at = ?,
               updated_at       = ?
         WHERE id = ?
        """,
        (_now_jst_str(), _now_jst_str(), item_id),
    )
    con.commit()

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
# 動画合成ユーティリティ
# =========================
def die(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise RuntimeError(msg)

def ensure_tools() -> None:
    if shutil.which("ffmpeg") is None:
        die("ffmpeg が見つかりません（brew install ffmpeg 等）。")

def run(cmd: List[str], log_path: Path) -> None:
    print("[CMD]", " ".join(cmd))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = p.stdout or ""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(out, encoding="utf-8")
    if p.returncode != 0:
        print(out)
        raise RuntimeError("Command failed")

def audio_duration_sec_wav(path: Path) -> float:
    if not path.exists():
        die(f"音声が見つかりません: {path}")
    with wave.open(str(path), "rb") as wf:
        frames = wf.getnframes()
        rate = wf.getframerate()
        return 0.0 if rate == 0 else frames / float(rate)

def collect_main_images(main_dir_candidates: List[Path], main_single_candidates: List[Path]) -> List[Path]:
    for d in main_dir_candidates:
        if d.exists():
            imgs = [p for p in sorted(d.iterdir()) if p.is_file() and p.suffix.lower() in IMAGE_EXTS]
            if imgs:
                return imgs
    for p in main_single_candidates:
        if p.exists():
            return [p]
    die(f"メイン画像が見つかりません: {main_dir_candidates} / {main_single_candidates}")
    return []

def allocate_equal_durations(total: float, n: int) -> List[float]:
    if n <= 0:
        return []
    per = total / n
    ds = [per] * n
    ds[-1] = max(0.0, total - sum(ds[:-1]))
    return ds

def make_concat_list(images: List[Path], durations: List[float], out_txt: Path) -> None:
    if len(images) != len(durations):
        die("images と durations の長さが一致しません。")
    lines: List[str] = []
    for img, d in zip(images, durations):
        lines.append(f"file '{str(img)}'")
        lines.append(f"duration {d:.6f}")
    lines.append(f"file '{str(images[-1])}'")
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")

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
# filter_complex 生成
# =========================
def _build_fc_video(T: float) -> str:
    # ★最後を [v]; で終わらせて、音声チェーンと確実に区切るのが重要
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
# ビルド本体
# =========================
def run_build(parent_dir: Path, item_id: int) -> Path:
    ensure_tools()

    base_dir = parent_dir
    title_img = base_dir / "image" / "title" / "title.png"
    comment_dir = base_dir / "image" / "comment"
    voice_dir = base_dir / "voice"
    voice_all = voice_dir / "all.wav"

    main_dir_candidates = [base_dir / "image" / "main", base_dir / "image" / "Main"]
    main_single_candidates = [
        base_dir / "image" / "main_image.jpeg",
        base_dir / "image" / "Main" / "1.jpeg",
        base_dir / "image" / "Main" / "1.jpg",
        base_dir / "image" / "Main" / "1.png",
    ]

    movie_dir = base_dir / "movie"
    out_mp4 = movie_dir / "youtube_upload.mp4"

    log_dir = base_dir / "_logs"
    ffmpeg_log = log_dir / "ffmpeg_build.log"
    ffmpeg_log_noduck = log_dir / "ffmpeg_build_noduck.log"

    tmp_video = TMP_DIR / "youtube_upload_tmp.mp4"
    tmp_audio_desc = TMP_DIR / "all_desc.wav"

    if not base_dir.exists():
        die(f"対象フォルダが見つかりません: {base_dir}")
    if not title_img.exists():
        die(f"タイトル画像が見つかりません: {title_img}")
    if not voice_all.exists():
        die(f"all.wav が見つかりません: {voice_all}")

    movie_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    ranks = collect_comment_ranks(comment_dir)
    print("[INFO] ranks (desc):", ranks[:10], "..." if len(ranks) > 10 else "")

    voice_map = build_voice_map(voice_dir)

    comment_imgs: List[Path] = []
    comment_durs: List[float] = []
    seg_paths_desc: List[Path] = []

    for r in ranks:
        img = comment_dir / f"{r}.png"
        if not img.exists():
            found = None
            for ext in (".png", ".jpg", ".jpeg", ".webp"):
                p = comment_dir / f"{r}{ext}"
                if p.exists():
                    found = p
                    break
            if found is None:
                die(f"コメント画像がありません: {comment_dir}/{r}.(png/jpg/jpeg/webp)")
            img = found

        if r not in voice_map:
            die(f"対応する分割音声がありません: voice/{r}_xxxxms.wav が必要です。")

        ms, seg_path = voice_map[r]
        comment_imgs.append(img)
        comment_durs.append(ms / 1000.0)
        seg_paths_desc.append(seg_path)

    audio_for_video = voice_all
    if AUTO_BUILD_AUDIO_DESC:
        concat_wavs(seg_paths_desc, tmp_audio_desc)
        audio_for_video = tmp_audio_desc
        print(f"[INFO] built desc audio: {audio_for_video}")

    T = audio_duration_sec_wav(audio_for_video)
    if T <= 0:
        die("音声の長さが0秒です。")
    print(f"[INFO] audio duration: {T:.3f}s")

    if comment_durs:
        adj = T - sum(comment_durs[:-1])
        comment_durs[-1] = max(0.0, adj)

    bgm_path, bgm_start = pick_bgm_by_id(item_id)
    if bgm_path:
        parity = "odd" if (item_id % 2 == 1) else "even"
        print(f"[INFO] BGM selected: id={item_id} ({parity}) -> {bgm_path.name} (start={bgm_start:.3f}s)")
    else:
        print(f"[INFO] BGM: (none) id={item_id}")

    main_imgs = collect_main_images(main_dir_candidates, main_single_candidates)
    main_durs = allocate_equal_durations(T, len(main_imgs))

    tmp = base_dir / "_build_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    main_list = tmp / "main_concat.txt"
    comment_list = tmp / "comment_concat.txt"
    make_concat_list(main_imgs, main_durs, main_list)
    make_concat_list(comment_imgs, comment_durs, comment_list)

    ffmpeg_out = out_mp4
    if WRITE_TO_LOCAL_TMP:
        ffmpeg_out = tmp_video

    def build_cmd(ducking: bool) -> Tuple[List[str], str]:
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
        cmd += [str(ffmpeg_out)]
        return cmd, fc

    # 1) まずダッキングありで試す
    try:
        cmd1, _ = build_cmd(ducking=bool(BGM_DUCKING))
        run(cmd1, log_path=ffmpeg_log)
    except Exception:
        # 2) ダッキングで落ちる環境ならノーダックへ
        if bgm_path and BGM_DUCKING:
            print("[WARN] ffmpeg failed with ducking. Retrying without ducking...")
            cmd2, _ = build_cmd(ducking=False)
            run(cmd2, log_path=ffmpeg_log_noduck)
        else:
            raise

    if WRITE_TO_LOCAL_TMP:
        safe_copy(ffmpeg_out, out_mp4)

    print("=== DONE ===")
    print(f"OUTPUT: {out_mp4}")
    print(f"FFMPEG LOG: {ffmpeg_log}")
    if ffmpeg_log_noduck.exists():
        print(f"FFMPEG LOG (NODUCK): {ffmpeg_log_noduck}")
    print(f"TMP: {tmp}（問題なければ削除OK）")

    return out_mp4

# =========================
# メイン（DBキューで1件拾って処理→check_create更新）
# =========================
def main() -> int:
    if not DB_PATH.exists():
        print(f"[ERROR] DB not found: {DB_PATH}", file=sys.stderr)
        return 2

    try:
        with _connect(DB_PATH) as con:
            _ensure_columns(con)

            row = _pick_one(con)
            if row is None:
                print(f"[INFO] no item with check_create={PICK_STATUS}.")
                return 0

            item_id = int(row["id"])
            folder_name = str(row["folder_name"])
            parent_dir = BASE_OUTPUT_ROOT / folder_name

            print(f"[INFO] picked id={item_id} folder_name={folder_name}")
            print(f"[INFO] parent_dir={parent_dir}")

            try:
                if not parent_dir.exists():
                    raise FileNotFoundError(f"parent_dir not found: {parent_dir}")

                out_mp4 = run_build(parent_dir, item_id=item_id)
                print(f"[OK] created: {out_mp4}")

                _update_status(con, item_id=item_id, status=DONE_STATUS, last_error=None)
                _mark_video_created(con, item_id=item_id)
                print(f"[OK] done. check_create {PICK_STATUS} -> {DONE_STATUS} (id={item_id})")
                return 0

            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                _update_status(con, item_id=item_id, status=PICK_STATUS, last_error=err)
                print(f"[ERROR] failed id={item_id} kept check_create={PICK_STATUS}. {err}", file=sys.stderr)
                return 1

    except Exception as e:
        print(f"[ERROR] DB operation failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 3

if __name__ == "__main__":
    raise SystemExit(main())
