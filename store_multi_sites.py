"""
store_multi_sites.py

このスクリプトは、Wikidot の複数サイト (EN, JP, CN, KO など) から
ページを取得し、(site, fullname) を主キーとする SQLite データベースに
保存するためのものです。
同じ fullname があっても、site が異なれば上書きされません。
"""

import os
import sqlite3
import xmlrpc.client
import logging
import socket
from typing import Any, Dict, List, Optional, Tuple, cast

import defusedxml.xmlrpc
from dotenv import load_dotenv
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

defusedxml.xmlrpc.monkey_patch()

# Set up logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ------------------------------------------------------------------------------
# ここで一度に取得したいサイト名をリスト化
# 好きなだけ追加可能
# ------------------------------------------------------------------------------
SITES = [
    "scp-wiki",  # EN (本家)
    "scp-jp",  # 日本支部
    "scp-wiki-cn",  # 中国支部
    "scpko",  # 韓国支部
]

# pages.select() / get_meta() / get_one() の一括取得サイズ
CHUNK_SIZE = 10

# 再試行設定（共通パラメータ）
MAX_RETRY_ATTEMPTS = 10  # 再試行回数を5回から10回に増加
MAX_WAIT_TIME = 180  # 最大待機時間を60秒から180秒に増加

# ------------------------------------------------------------------------------
# 環境変数 (.env) からキーやユーザー名を読み込む
# WIKIDOT_API_USER / WIKIDOT_API_KEY / DB_FILE
# ------------------------------------------------------------------------------
load_dotenv()
API_USER = os.getenv("WIKIDOT_API_USER", "your-username")
API_KEY = os.getenv("WIKIDOT_API_KEY", "your-api-key")
DB_FILE = os.getenv("DB_FILE", "data/scp_data.sqlite")


@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type((ConnectionError, socket.gaierror)),
)
def get_server_proxy(user: str, key: str) -> xmlrpc.client.ServerProxy:
    """
    Wikidot API に認証付きで接続するための
    xmlrpc.client.ServerProxy を生成して返す。

    ネットワーク障害やDNS解決エラー（socket.gaierror）が発生した場合は
    指数バックオフで再試行する。
    """
    api_url = f"https://{user}:{key}@wikidot.com/xml-rpc-api.php"
    return xmlrpc.client.ServerProxy(api_url)


@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(
        (xmlrpc.client.Fault, ConnectionError, socket.gaierror)
    ),
)
def select_all_pages(site: str, server: xmlrpc.client.ServerProxy) -> List[str]:
    """
    対象サイト (site) から全ページの fullname リストを取得する。

    レート制限エラーやネットワーク障害が起きた場合は指数バックオフで再試行する。
    """
    return cast(List[str], server.pages.select({"site": site}))


@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(
        (xmlrpc.client.Fault, ConnectionError, socket.gaierror)
    ),
)
def get_pages_meta(
    site: str, server: xmlrpc.client.ServerProxy, pages: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    同時に最大10件まで pages.get_meta() でメタデータを取得。
    updated_at, revisions, rating などがまとめて返る。

    レート制限エラーやネットワーク障害が起きた場合は指数バックオフで再試行する。
    DNS解決エラー（socket.gaierror）も同様に再試行する。

    Returns:
        meta_info: Dict where keys are page names, and values are dictionaries
        containing metadata (fullname, updated_at, tags, etc.).
    """
    return cast(
        Dict[str, Dict[str, Any]], server.pages.get_meta({"site": site, "pages": pages})
    )


@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(
        (xmlrpc.client.Fault, ConnectionError, socket.gaierror)
    ),
)
def get_one_page(
    site: str, server: xmlrpc.client.ServerProxy, page_name: str
) -> Dict[str, Any]:
    """
    1ページ分の詳細情報を取得する。
    content や html、rating、tags などが含まれる。

    レート制限エラーやネットワーク障害が起きた場合は指数バックオフで再試行する。
    DNS解決エラー（socket.gaierror）も同様に再試行する。

    Returns:
        A dictionary containing page details (content, html, rating, tags, etc.).
    """
    return cast(Dict[str, Any], server.pages.get_one({"site": site, "page": page_name}))


def create_tables(conn: sqlite3.Connection) -> None:
    """
    SQLite DB にテーブル (pages, page_tags) を作成する。
    site と fullname を組みにした複合主キーで
    同名ページでも site が違えば衝突しない。
    """
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS pages (
          site            TEXT NOT NULL,
          fullname        TEXT NOT NULL,
          title           TEXT,
          created_at      TEXT,
          created_by      TEXT,
          updated_at      TEXT,
          updated_by      TEXT,
          parent_fullname TEXT,
          parent_title    TEXT,
          rating          INTEGER,
          revisions       INTEGER,
          children        INTEGER,
          comments        INTEGER,
          commented_at    TEXT,
          commented_by    TEXT,
          content         TEXT,
          html            TEXT,
          PRIMARY KEY (site, fullname)
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS page_tags (
          site     TEXT NOT NULL,
          fullname TEXT NOT NULL,
          tag      TEXT NOT NULL,
          PRIMARY KEY (site, fullname, tag)
        )
        """
    )

    conn.commit()


def insert_page(conn: sqlite3.Connection, site: str, page_data: Dict[str, Any]) -> None:
    """
    1ページ分のデータを pages テーブルに INSERT (または REPLACE) する。
    site を含めて複合主キーにするので、同じページ名でも site が違えば衝突しない。
    """
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO pages (
          site,
          fullname,
          title,
          created_at,
          created_by,
          updated_at,
          updated_by,
          parent_fullname,
          parent_title,
          rating,
          revisions,
          children,
          comments,
          commented_at,
          commented_by,
          content,
          html
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site,
            page_data.get("fullname"),
            page_data.get("title"),
            page_data.get("created_at"),
            page_data.get("created_by"),
            page_data.get("updated_at"),
            page_data.get("updated_by"),
            page_data.get("parent_fullname"),
            page_data.get("parent_title"),
            page_data.get("rating", 0),
            page_data.get("revisions", 0),
            page_data.get("children", 0),
            page_data.get("comments", 0),
            page_data.get("commented_at"),
            page_data.get("commented_by"),
            page_data.get("content"),
            page_data.get("html"),
        ),
    )
    conn.commit()


def insert_tags(
    conn: sqlite3.Connection,
    site: str,
    page_fullname: str,
    tags_list: List[str],
) -> None:
    """
    1ページに付いているタグ (tags) のリストを
    page_tags テーブルに INSERT (または REPLACE) する。
    """
    c = conn.cursor()
    for tag in tags_list:
        c.execute(
            """
            INSERT OR REPLACE INTO page_tags (site, fullname, tag)
            VALUES (?, ?, ?)
            """,
            (site, page_fullname, tag),
        )
    conn.commit()


def get_db_page_info(
    conn: sqlite3.Connection, site: str, page_name: str
) -> Optional[Tuple[str, int]]:
    """
    DBに既にあるページの updated_at, revisions を返す。
    まだ存在しない場合は None を返す。

    Returns:
        (updated_at, revisions) if the page exists in DB, otherwise None.
    """
    c = conn.cursor()
    row = c.execute(
        """
        SELECT updated_at, revisions
        FROM pages
        WHERE site=? AND fullname=?
        """,
        (site, page_name),
    ).fetchone()
    # row is either (updated_at, revisions) or None
    return row  # type: ignore


def main() -> None:
    """
    メイン処理:
    1) DBを開いてテーブルを作成
    2) SITES に列挙された各サイトから全ページを取得
    3) 全ページをCHUNKごとにメタデータ(get_pages_meta)だけ先に拾う
    4) DBの既存データと比較し、更新されてるページだけ get_one_page で本体を取得
    5) 固定の待機時間を設けず、エラー発生時のみ指数バックオフでリトライ
    6) 取得した情報をDBにINSERT
    7) ページが削除されていた場合(Fault 406)はスキップ
    8) ネットワークエラーやDNS解決エラー(socket.gaierror)は粘り強く再試行
    """
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)

    logger.info(f"DB_FILE = {DB_FILE}")
    try:
        server = get_server_proxy(API_USER, API_KEY)
    except Exception as e:
        logger.error(f"サーバー接続エラー: {e}")
        logger.info(
            "プログラムを終了します。ネットワーク接続を確認してから再実行してください。"
        )
        return

    for site in SITES:
        logger.info(f"=== 開始: {site} ===")
        try:
            all_pages = select_all_pages(site, server)
            logger.info(f"  => {len(all_pages)} ページを取得しました (site={site})")
        except Exception as e:
            logger.error(f"{site} のページ一覧取得に失敗: {e}")
            logger.info(f"{site} をスキップして次のサイトへ進みます")
            continue

        processed_count = 0
        total_pages = len(all_pages)

        # ページ名をCHUNK_SIZE ごとに小分けして処理
        for i in range(0, total_pages, CHUNK_SIZE):
            chunk = all_pages[i : i + CHUNK_SIZE]

            # メタデータをまとめて取得
            # meta_info は { 'page_name': { 'fullname':..., 'updated_at':..., 'revisions':..., 'tags': [...], ... }, ... }
            try:
                meta_info = get_pages_meta(site, server, chunk)
            except (xmlrpc.client.Fault, ConnectionError, socket.gaierror) as e:
                logger.warning(f"get_pages_meta失敗: {e}")
                # tenacityによる再試行が全て失敗した場合、このチャンクをスキップ
                continue
            except Exception as e:
                logger.warning(f"予期せぬエラー in get_pages_meta: {e}")
                continue

            if not meta_info:
                # 万が一何も取れんかったらスキップ
                continue

            for page_name in chunk:
                processed_count += 1
                # Replace sys.stdout.write with a progress indicator
                if processed_count % 10 == 0 or processed_count == total_pages:
                    logger.info(
                        f"Processing {processed_count}/{total_pages} for {site}..."
                    )

                meta = meta_info.get(page_name)
                if not meta:
                    # 何故かこのページだけメタ情報が無い場合はスキップ
                    continue

                # DB上の更新日時/リビジョンと比較して、同じならスキップ
                db_row = get_db_page_info(conn, site, page_name)
                if db_row is not None:
                    db_updated_at, db_revisions = db_row
                    if db_updated_at == meta.get(
                        "updated_at"
                    ) and db_revisions == meta.get("revisions", 0):
                        # 変化なし → skip
                        continue

                # 変化あり or まだDBに無い → get_one_page
                try:
                    fullinfo = get_one_page(site, server, page_name)
                except xmlrpc.client.Fault as fault:
                    if (
                        fault.faultCode == 406
                        and "page does not exist" in fault.faultString.lower()
                    ):
                        logger.info(
                            f"ページ '{page_name}' は削除されてるみたいやからスキップ"
                        )
                        continue
                    else:
                        # それ以外の既知のエラーは警告を出してスキップ
                        logger.warning(
                            f"ページ '{page_name}' の取得中にエラー: {fault}"
                        )
                        continue
                except (ConnectionError, socket.gaierror) as e:
                    # ネットワークエラーはtenacityでの再試行が全て失敗した場合
                    logger.warning(
                        f"ページ '{page_name}' の取得中にネットワークエラー: {e}"
                    )
                    continue
                except Exception as e:
                    # 予期せぬエラーは警告を出してスキップ
                    logger.warning(
                        f"ページ '{page_name}' の取得中に予期せぬエラー: {e}"
                    )
                    continue

                # get_one_page で拾った情報 + meta_data (tags など) をマージ
                if "tags" in meta:
                    fullinfo["tags"] = meta["tags"]

                # DBにINSERT
                try:
                    insert_page(conn, site, fullinfo)
                    insert_tags(conn, site, page_name, fullinfo.get("tags", []))
                except sqlite3.Error as e:
                    logger.warning(f"DB挿入エラー for '{page_name}': {e}")
                    continue

            # chunk単位のループ終わり

        logger.info(f"=== {site} の処理終了: 合計 {processed_count} ページ ===")

    conn.close()
    logger.info("完了: すべてのサイトのページ取得が完了しました。")


if __name__ == "__main__":
    main()
