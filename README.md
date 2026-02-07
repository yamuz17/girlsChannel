# girlsChannel (暫定)

このリポジトリは `01_暫定_20260102` を正として運用するための暫定一式です。

## 目的
ガールズチャンネルのトピック取得 → 画像生成 → 音声生成 → サムネ/preview → 組み立ての一連処理を
DBキュー（`check_create`）で管理して自動実行します。

## フォルダ構成
- `01_暫定_20260102/` 実行対象のスクリプト群
- `01_暫定_20260102/.env.example` 環境変数テンプレート

## 前提
- Python 3.10+（推奨）
- Playwright（`02_データ取得.py` で使用）
- 依存ライブラリ（Pillow / requests など）

※ 依存インストールは環境に合わせて行ってください。

## セットアップ
1. `01_暫定_20260102/.env.example` を `01_暫定_20260102/.env` にコピー
2. `.env` に各パスや設定値を記入

最低限の必須キー:
- `DB_PATH`
- `BASE_OUTPUT_ROOT`
- `SCRIPTS_DIR`

## 実行
```bash
python3 01_暫定_20260102/01_ランチャー.py --runs 5
```

## スクリプト一覧
- `01_ランチャー.py`  全体制御
- `02_データ取得.py`  トピック取得・DB更新
- `03_画像生成.py`  画像生成
- `04_音声生成.py`  音声生成
- `05_サムネ動画作成.py`  サムネ/preview
- `99_パーツ組み立て.py`  最終組み立て

## .env の主なキー
- DB: `DB_PATH`, `TABLE_NAME`
- 出力: `BASE_OUTPUT_ROOT`, `SCRIPTS_DIR`
- ステージ: `STA_02`〜`END_99`
- Playwright: `HEADLESS_MODE`, `WAIT_TIMEOUT_MS`
- 音声: `ENGINE_URL`, `TOTAL_VIDEO_SEC` など

## 補足
- DBや出力先は `.env` の `DB_PATH` / `BASE_OUTPUT_ROOT` で指定します。
- 各スクリプトは `.env` から設定を読み込みます。
