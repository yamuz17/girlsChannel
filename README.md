# girlsChannel

このリポジトリは `99_` を実行対象として運用します。

## 目的
ガールズチャンネルのトピック取得 → 画像生成 → 音声生成 → サムネ/preview → 組み立ての一連処理を
DBキュー（`check_create`）で管理して自動実行します。

## フォルダ構成
- `99_/` 実行対象のスクリプト群（番号なし）
- `girlsChannel.env` リポジトリ共通の環境設定（Gitには含めない）

## 前提
- Python 3.10+（推奨）
- Playwright（`02_データ取得.py` で使用）
- 依存ライブラリ（Pillow / requests など）

※ 依存インストールは環境に合わせて行ってください。

## セットアップ
1. `girlsChannel.env` を作成し、パスなどを記入
2. `99_/` のスクリプトは「1つ上の階層の `girlsChannel.env`」を参照します
3. もしリポジトリ直下に無い場合は `~/Documents/readOnly/girlsChannel.env` を参照します

最低限の必須キー:
- `DB_PATH`
- `BASE_OUTPUT_ROOT`
- `SCRIPTS_DIR`

## 実行フロー（推奨）
1. リスト作成（DB初期化・新規取得）
2. パイプライン実行（動画作成）
3. 投稿予約（YouTubeアップロード）

## 実行
### 1) リスト作成（DBを作成/更新）
単一カテゴリ:
```bash
python3 99_/build_list.py
```
複数カテゴリ:
```bash
python3 99_/build_list_multi.py
```

### 2) パイプライン実行（動画作成）
```bash
python3 99_/run_pipeline.py --runs 5
```

### 3) 投稿予約（アップロード）
```bash
python3 99_/投稿予約.py
```

## スクリプト一覧
- `run_pipeline.py`  全体制御（1つの実行ボタンで順番に回す）
- `fetch_data.py`  トピック取得・DB更新
- `make_images.py`  画像生成
- `make_audio.py`  音声生成
- `make_preview.py`  サムネ/preview
- `assemble_video.py`  最終組み立て
- `build_list.py`  DB作成/更新（単一カテゴリ）
- `build_list_multi.py`  DB作成/更新（複数カテゴリ）
- `投稿予約.py`  投稿予約/アップロード
- `投稿予約2.py`  投稿予約/アップロード（別仕様版）

## girlsChannel.env の主なキー
- DB: `DB_PATH`, `TABLE_NAME`
- 出力: `BASE_OUTPUT_ROOT`, `SCRIPTS_DIR`
- ステージ: `STA_02`〜`END_99`
- Playwright: `HEADLESS_MODE`, `WAIT_TIMEOUT_MS`
- 音声: `ENGINE_URL`, `TOTAL_VIDEO_SEC` など
- ランチャー: `SCRIPT_02_NAME`〜`SCRIPT_99_NAME`

## 補足
- DBや出力先は `girlsChannel.env` の `DB_PATH` / `BASE_OUTPUT_ROOT` で指定します。
- 各スクリプトは `girlsChannel.env` から設定を読み込みます。
