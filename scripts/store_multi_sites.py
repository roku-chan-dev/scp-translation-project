#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
store_multi_sites.py

このスクリプトは、Wikidot の複数サイト (EN, JP, CN, KO など) から
ページを取得し、(site, fullname) を主キーとする SQLite データベースに
保存するためのものです。
同じ fullname があっても、site が異なれば上書きされません。
"""

import os
import time
import sys
import sqlite3
import xmlrpc.client
from dotenv import load_dotenv
from retrying import retry

# ------------------------------------------------------------------------------
# ここで一度に取得したいサイト名をリスト化
# 好きなだけ追加可能
# ------------------------------------------------------------------------------
SITES = [
    "scp-wiki",     # EN (本家)
    "scp-jp",       # 日本支部
    "scp-wiki-cn",  # 中国支部
    "scpko"         # 韓国支部 (本当は "scp-kr" かも。Wiki のドメイン要確認)
]

# API の呼び出し間隔(秒) - Wikidot APIは 240req/min 制限があるのでウェイトを置く
REQUEST_INTERVAL = 0.4

# pages.get_meta() / pages.get_one() の一括取得サイズ
CHUNK_SIZE = 10

# ------------------------------------------------------------------------------
# 環境変数 (.env) からキーやユーザー名を読み込む
# WIKIDOT_API_USER / WIKIDOT_API_KEY / DB_FILE
# ------------------------------------------------------------------------------
load_dotenv()
API_USER = os.getenv("WIKIDOT_API_USER", "your-username")
API_KEY = os.getenv("WIKIDOT_API_KEY", "your-api-key")
DB_FILE = os.getenv("DB_FILE", "data/scp_data.sqlite")


def get_server_proxy(user, key):
    """
    Wikidot API に認証付きで接続するための
    xmlrpc.client.ServerProxy を生成して返す。
    """
    api_url = f"https://{user}:{key}@wikidot.com/xml-rpc-api.php"
    return xmlrpc.client.ServerProxy(api_url)


@retry(stop_max_attempt_number=3,
       wait_exponential_multiplier=1000,
       wait_exponential_max=60000)
def select_all_pages(site, server):
    """
    対象サイト (site) から全ページの fullname リストを取得する。
    """
    return server.pages.select({"site": site})


@retry(stop_max_attempt_number=3,
       wait_exponential_multiplier=1000,
       wait_exponential_max=60000)
def get_one_page(site, server, page_name):
    """
    1ページ分の詳細情報を取得する。
    content や html、rating、tags などが含まれる。
    """
    return server.pages.get_one({"site": site, "page": page_name})


def create_tables(conn):
    """
    SQLite DB にテーブル (pages, page_tags) を作成する。
    site と fullname を組みにした複合主キーで
    同名ページでも site が違えば衝突しない。
    """
    c = conn.cursor()

    c.execute("""
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
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS page_tags (
          site     TEXT NOT NULL,
          fullname TEXT NOT NULL,
          tag      TEXT NOT NULL,
          PRIMARY KEY (site, fullname, tag)
        )
    """)

    conn.commit()


def insert_page(conn, site, page_data):
    """
    1ページ分のデータを pages テーブルに INSERT (または REPLACE) する。
    site を含めて複合主キーにするので、上書きの心配は少ない。
    """
    c = conn.cursor()
    c.execute("""
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
    """, (
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
        page_data.get("html")
    ))
    conn.commit()


def insert_tags(conn, site, page_fullname, tags_list):
    """
    1ページに付いているタグ (tags) のリストを
    page_tags テーブルに INSERT (または REPLACE) する。
    """
    c = conn.cursor()
    for tag in tags_list:
        c.execute("""
            INSERT OR REPLACE INTO page_tags (site, fullname, tag)
            VALUES (?, ?, ?)
        """, (site, page_fullname, tag))
    conn.commit()


def main():
    """
    メイン処理:
    1) DBを開いてテーブルを作成
    2) SITES に列挙された各サイトから全ページを取得
    3) 1ページごとに詳細情報を拾って DB に格納
    4) リクエスト間隔を空けてレート制限を回避
    """
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)

    server = get_server_proxy(API_USER, API_KEY)

    for site in SITES:
        print(f"\n=== 開始: {site} ===")
        all_pages = select_all_pages(site, server)
        print(f"  => {len(all_pages)} ページを取得しました (site={site})")

        processed_count = 0
        total_pages = len(all_pages)

        # ページ名をCHUNK_SIZE ごとに小分けして処理
        for i in range(0, total_pages, CHUNK_SIZE):
            chunk = all_pages[i: i + CHUNK_SIZE]
            # get_pages_meta() を使わない場合はコメントアウトでOK
            # meta_data = get_pages_meta(site, server, chunk)

            # 連続リクエストの速度を調整
            time.sleep(REQUEST_INTERVAL)

            for page_name in chunk:
                processed_count += 1
                page_data = get_one_page(site, server, page_name)
                time.sleep(REQUEST_INTERVAL)

                insert_page(conn, site, page_data)
                insert_tags(conn, site, page_name, page_data.get("tags", []))

                # 進捗表示 (上書き)
                sys.stdout.write(
                    f"\rProcessed {processed_count}/{total_pages} for {site}..."
                )
                sys.stdout.flush()

        print(f"\n=== {site} の処理終了: 合計 {processed_count} ページ ===")

    conn.close()
    print("\n[完了] すべてのサイトのページ取得が完了しました。")


if __name__ == "__main__":
    main()
