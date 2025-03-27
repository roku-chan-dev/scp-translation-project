# memories.md

## 1. プロジェクト概要 / 目指すゴール

- **SCP Wiki**（EN, JP, CN, KO など複数支部）の記事データを、Wikidot API を通じて取得し、ローカルの SQLite データベースに蓄えるプロジェクト。
- 膨大な SCP 記事を一元管理し、翻訳や RAG（Retrieval Augmented Generation）の構築に役立てることが最終目的。
- 差分更新や削除ページ対応など、現実的な運用を可能にすることを重視。

---

## 2. 課題・背景

1. **大量ページ取得の非効率**
   - 単純に全ページを最初から取得し続けると時間がかかり過ぎる。
   - 一度途中で止まると再開が面倒。
2. **API エラーでスクリプト停止**
   - ページが削除されると Wikidot API から Fault 406 が返り、スクリプト全体が止まる状況。
3. **コード整形や docstring スタイル**
   - コードを綺麗に保ちたい → Black + isort + flake8 を導入。
   - docstring を Google スタイルに統一したい → flake8-docstrings でチェック。

このあたりの課題を順番に解決していく流れで検討を進めた。

---

## 3. 話し合いの経緯と最終的な決定事項

### 3.1 大量ページの差分更新

- **決定**:
  1. まず `pages.get_meta()` で「updated_at」「revisions」等のメタデータを取得。
  2. DB に既に同じ `(site, fullname)` が存在し、かつ `updated_at` & `revisions` が同一なら本体取得をスキップ。
  3. 変わっているページ or 新規ページだけ `pages.get_one()` して DB を更新。
- **利点**:
  - 初回は全件取得だが、2 回目以降は変更のあったページだけを再取得 → 時間短縮
  - 途中で止まっても再実行すれば「未更新のページはすぐスキップ → 続きから再開」状態になる。

### 3.2 ページ削除(Fault 406)の処理

- **決定**:
  - `try-except xmlrpc.client.Fault` でエラーをキャッチ。
  - Fault 406 & `'page does not exist'` がメッセージに含まれていれば、そのページをスキップして処理を続ける。
  - 他のエラー（ネットワーク障害など）は再 raise して原因を追う。

### 3.3 テーブル構造と DB 運用

- **テーブル**:
  - `pages(site, fullname, updated_at, revisions, content, html, …)`
    - 複合主キー `(site, fullname)`
    - `INSERT OR REPLACE` により重複分は上書き
  - `page_tags(site, fullname, tag)`
- **DB ファイル**:
  - 大きくなる恐れがあるので `.gitignore` に入れてコミットしない
  - 画像や添付ファイルは扱わずテキストのみ

### 3.4 コード整形・Lint・docstring

- **VSCode 設定**:
  - `editor.formatOnSave = true`, `python.formatting.provider = "black"`,  
    `python.linting.flake8Enabled = true`, …
- **Black & isort** で自動整形 → 余計なフォーマット議論を省略
- **flake8 + flake8-docstrings**
  - `.flake8` で `docstring-convention=google` を指定 → Google スタイル docstring のチェック
- **docstring** には Args:, Returns: を書き、「何を返して、何を受け取るか」読みやすくする

---

## 4. 実装概要

### 4.1 store_multi_sites.py

1. **サイト一覧** `SITES = [...]` を定義
2. **select_all_pages(site)** でページ fullname のリスト取得
3. **chunk ごとに get_pages_meta** でメタデータ（updated_at, revisions, tags 等）
4. DB にあるか確認し、差分があれば `get_one_page` → `insert_page`, `insert_tags`
5. ページ削除の場合は except 406 → スキップ
6. 最後まで回すと DB が最新化される

この仕組みにより、

- 初回: 全部取得
- 2 回目以降: 変更分だけ取得/更新
- 中断後の再実行: ほとんどスキップで高速に完走

### 4.2 その他スクリプト

- **export_scp_data.py**（予定/例）:
  - DB にある SCP 記事を `.wikidot` + `.json` に書き出す
  - RAG や翻訳支援のため、ファイル単位で管理
- **その他**:
  - スクリプトや設定ファイルが増える場合は `scripts/` フォルダにまとめる。

---

## 5. 今後の TODO

1. **RAG 構築**
   - Embedding データベース(Haystack や FAISS)に本文を登録して検索する
   - GPT などの LLM に「関連コンテキスト」を注入するフローを整備
2. **翻訳フロー**
   - 英文 → Google 翻訳等 → `.wikidot`仮訳 → DB に入れて査読支援を実装するアイデア
3. **HTML→Markdown 変換**
   - wikidot 構文をどう扱うか？ 余裕があれば変換ツールを作る
4. **アーカイブされる削除ページの扱い**
   - 削除されたページを DB からも削除する？ “deleted_at” カラムを設けて残す？ などさらなる詳細設計は今後の検討。

---

## 6. 注意点・運用ルール

- **Wikidot API 制限**
  - 1 分あたり 240req → `REQUEST_INTERVAL=0.4` でレートを保つ
- **途中で止まったら**
  - そのまま `python store_multi_sites.py` を再度実行すれば OK。 差分更新で高速に続行
- **DB の肥大化**
  - 毎回同じ記事を上書き保存しても基本壊れないが、古いレコードは持たない。 バックアップは必要に応じて
- **Lint・docstring**
  - コード修正したら保存時に Black & isort が走る → 同じ書式を維持
  - flake8 エラーはなるべく放置せず対応（docstring 不足など）

---

## 7. 雑記・ヒント

- **Fault 406**：実際の利用中、「存在しないページ」扱いはたまにある。 リネームや削除でこうなるらしい。
- **SCP-KO** の正式サイト名は `scp-kr` という説？ リサーチが要るかも
- **Pull Request & レビュー**： 大きい変更時には PR で議論。リポジトリ参加者が増えた場合に備える。
- **限界**： Wiki のページ数が増えすぎると DB ファイルが巨大になる → Partitioning や外部 DB（Elasticsearch 等）への移行を考慮してもいい。

---

## 8. おわりに

この **`memories.md`** では、ふたりの会話を通じて得られた知見や決定事項を簡潔にまとめた。  
もし新しいメンバーや未来の自分が本プロジェクトに触れるときは、まずこのファイルを読んで**全体像**を把握してからコードに入ってほしい。  
何か大きな変更をするときには、このファイルに **「どんな背景で、何を変えたか」** を追記していくと、プロジェクトの成長が綺麗に履歴として残るはず。

以上！
