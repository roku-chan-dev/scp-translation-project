"""
store_multi_sites.py

Fetches pages from multiple Wikidot sites (EN, JP, CN, KO, etc.)
using the Wikidot API and stores them in an SQLite database.
Uses (site, fullname) as the primary key to differentiate pages
from different sites even if they share the same fullname.
Implements differential updates based on metadata to improve efficiency.
"""

import http.client
import logging
import os
import socket
import sqlite3
import time
import xmlrpc.client
import datetime  # Import datetime module
from typing import Any, Dict, List, Optional, Tuple, cast

# Security patch for xmlrpc
import defusedxml.xmlrpc
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

# Apply the security patch
defusedxml.xmlrpc.monkey_patch()

# --- Configuration ---
# Set up logger
# Use basicConfig for simplicity, consider file logging for long runs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# List of Wikidot site names to process
SITES = [
    "scp-wiki",  # English (main)
    "scp-jp",  # Japanese Branch
    "scp-wiki-cn",  # Chinese Branch
    "scpko",  # Korean Branch
    # Add more sites here as needed
]

# Chunk size for batch API calls (pages.select, get_meta)
CHUNK_SIZE = 10

# Retry settings for tenacity
MAX_RETRY_ATTEMPTS = 15
MAX_TOTAL_DELAY = 3600
MAX_WAIT_TIME = 600
MIN_WAIT_TIME = 2

# Load environment variables (.env file)
load_dotenv()
API_USER = os.getenv("WIKIDOT_API_USER", "your-username")
API_KEY = os.getenv("WIKIDOT_API_KEY", "your-api-key")
DB_FILE = os.getenv("DB_FILE", "data/scp_data.sqlite")
# --- End Configuration ---


# --- Wikidot API Interaction with Retries ---

# Define common retryable exceptions
RETRYABLE_EXCEPTIONS = (
    xmlrpc.client.Fault,  # Handles API errors like rate limits (503)
    ConnectionError,  # Handles network connection issues
    socket.gaierror,  # Handles DNS resolution errors
    socket.timeout,  # Handles socket timeouts
    xmlrpc.client.ProtocolError,  # Handles protocol errors (e.g., bad gateway)
    http.client.HTTPException,
)


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def get_server_proxy(user: str, key: str) -> xmlrpc.client.ServerProxy:
    """
    Creates an authenticated ServerProxy for the Wikidot API.

    Retries on common network and API errors using exponential backoff.

    Args:
        user: Wikidot username.
        key: Wikidot API key.

    Returns:
        An xmlrpc.client.ServerProxy instance.

    Raises:
        Catches and retries RETRYABLE_EXCEPTIONS. If retries fail,
        the last exception is reraised.
    """
    logger.debug("Attempting to connect to Wikidot API...")
    api_url = f"https://{user}:{key}@wikidot.com/xml-rpc-api.php"
    # Consider setting a timeout for the transport
    # transport = xmlrpc.client.SafeTransport()
    # transport.timeout = 60 # Example: 60 seconds timeout
    # proxy = xmlrpc.client.ServerProxy(api_url, transport=transport)
    proxy = xmlrpc.client.ServerProxy(api_url)
    # Optional: Test connection with a simple call like list_users
    # proxy.system.listMethods()
    logger.debug("Successfully created API proxy.")
    return proxy


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def select_all_pages(site: str, server: xmlrpc.client.ServerProxy) -> List[str]:
    """
    Fetches a list of all page fullnames for a given site.

    Retries on common network and API errors.

    Args:
        site: The Wikidot site name (e.g., 'scp-wiki').
        server: The authenticated ServerProxy instance.

    Returns:
        A list of page fullnames.

    Raises:
        Catches and retries RETRYABLE_EXCEPTIONS. Reraises if retries fail.
    """
    logger.debug("Fetching page list for site: %s", site)
    try:
        return cast(List[str], server.pages.select({"site": site}))
    except xmlrpc.client.Fault as e:
        if e.faultCode == 403:
            logger.warning(
                "API access forbidden for site '%s' (403). Skipping page fetch. "
                "Check 'Manage Site > API' settings on Wikidot.",
                site,
            )
            return []  # Return empty list to skip processing this site
        else:
            # Reraise other Fault exceptions for the @retry decorator to handle
            raise e


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def get_pages_meta(
    site: str, server: xmlrpc.client.ServerProxy, pages: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Fetches metadata for a list of pages (up to CHUNK_SIZE).

    Retries on common network and API errors.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        pages: A list of page fullnames to fetch metadata for.

    Returns:
        A dictionary where keys are page fullnames and values are
        dictionaries containing metadata (updated_at, revisions, tags, etc.).

    Raises:
        Catches and retries RETRYABLE_EXCEPTIONS. Reraises if retries fail.
    """
    logger.debug("Fetching metadata for %d pages on site %s", len(pages), site)
    return cast(
        Dict[str, Dict[str, Any]], server.pages.get_meta({"site": site, "pages": pages})
    )


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions (excluding Fault 403 handled below)
    retry=retry_if_exception_type(
        tuple(exc for exc in RETRYABLE_EXCEPTIONS if exc != xmlrpc.client.Fault)
        + (xmlrpc.client.Fault,) # Add Fault back, but handle 403 specifically
    ),
    # Log before each retry for monitoring (include page name if possible)
    # TODO: Enhance logging here if needed, maybe pass page_name to a custom logger
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def get_one_page(
    site: str, server: xmlrpc.client.ServerProxy, page_name: str
) -> Optional[Dict[str, Any]]: # Return type changed to Optional
    """
    Fetches the full content and details for a single page.

    Retries on common network and API errors, except for 403 Forbidden.
    If a 403 error occurs, logs a warning and returns None.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        page_name: The fullname of the page to fetch.

    Returns:
        A dictionary containing page details, or None if a 403 error occurred.

    Raises:
        Catches and retries RETRYABLE_EXCEPTIONS (excluding 403). Reraises if retries fail.
    """
    logger.debug("Fetching full content for page: %s on site %s", page_name, site)
    try:
        return cast(Dict[str, Any], server.pages.get_one({"site": site, "page": page_name}))
    except xmlrpc.client.Fault as e:
        if e.faultCode == 403:
            logger.warning(
                "Access forbidden (403) for page '%s' on site '%s'. Skipping page.",
                page_name,
                site,
            )
            return None  # Skip this page, do not retry
        else:
            # Reraise other Fault exceptions for the @retry decorator to handle
            logger.warning(
                "API Fault %s encountered for page '%s' on site '%s'. Retrying...",
                e.faultCode, page_name, site
            )
            raise e # Let tenacity handle retries for other Faults
    except RETRYABLE_EXCEPTIONS as e:
        # Log other retryable errors with page context before tenacity handles them
        logger.warning(
            "Error '%s' encountered for page '%s' on site '%s'. Retrying...",
            type(e).__name__, page_name, site
        )
        raise e # Let tenacity handle retries


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def fetch_all_categories(site: str, server: xmlrpc.client.ServerProxy) -> List[str]:
    """
    Fetches all categories for a given site.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.

    Returns:
        A list of category names.
    """
    logger.debug("Fetching all categories for site: %s", site)
    return cast(List[str], server.categories.select({"site": site}))


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def fetch_page_files(site: str, server: xmlrpc.client.ServerProxy, page_name: str) -> List[str]:
    """
    Fetches a list of all file names attached to a specific page.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        page_name: The full page name.

    Returns:
        A list of file names attached to the page.
    """
    logger.debug("Fetching file list for page: %s on site %s", page_name, site)
    return cast(List[str], server.files.select({"site": site, "page": page_name}))


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def fetch_files_meta(
    site: str, server: xmlrpc.client.ServerProxy, page_name: str, file_names: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Fetches metadata for a list of files attached to a page.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        page_name: The full page name.
        file_names: List of file names to fetch metadata for (max 10).

    Returns:
        A dictionary where keys are file names and values are metadata dictionaries.
    """
    logger.debug("Fetching metadata for %d files on page %s, site %s",
                len(file_names), page_name, site)
    return cast(
        Dict[str, Dict[str, Any]],
        server.files.get_meta({"site": site, "page": page_name, "files": file_names})
    )


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def fetch_page_comments(site: str, server: xmlrpc.client.ServerProxy, page_name: str) -> List[str]:
    """
    Fetches all comment IDs for a specific page.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        page_name: The full page name.

    Returns:
        A list of comment IDs.
    """
    logger.debug("Fetching comments for page: %s on site %s", page_name, site)
    return cast(List[str], server.posts.select({"site": site, "page": page_name}))


@retry(
    # Stop after either max attempts or max delay, whichever comes first
    stop=(stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_TOTAL_DELAY)),
    # Use exponential backoff with jitter to avoid thundering herd
    wait=wait_random_exponential(multiplier=1, min=MIN_WAIT_TIME, max=MAX_WAIT_TIME),
    # Retry only on specific exceptions
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    # Log before each retry for monitoring
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def fetch_comments_data(
    site: str, server: xmlrpc.client.ServerProxy, comment_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Fetches detailed data for a list of comment IDs.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        comment_ids: List of comment IDs to fetch data for (max 10).

    Returns:
        A dictionary where keys are comment IDs and values are comment data dictionaries.
    """
    logger.debug("Fetching data for %d comments on site %s", len(comment_ids), site)

    # Ensure comment_ids are all strings
    comment_ids_str = [str(comment_id) for comment_id in comment_ids]

    # API debug log
    logger.debug("Sending API request with posts: %s", comment_ids_str)

    return cast(
        Dict[str, Dict[str, Any]],
        server.posts.get({"site": site, "posts": comment_ids_str})
    )


# --- Database Operations ---


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Creates the necessary tables in the SQLite database
    if they don't already exist. Handles all data types available from Wikidot API.
    Also adds the deleted_at column if missing.
    """
    logger.info("Ensuring database tables exist...")
    cursor = conn.cursor()
    try:
        # Main table for page content and most metadata
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
              site            TEXT NOT NULL,
              fullname        TEXT NOT NULL,
              title           TEXT,
              created_at      TEXT, -- ISO 8601 format recommended
              created_by      TEXT,
              updated_at      TEXT, -- ISO 8601 format recommended
              updated_by      TEXT,
              parent_fullname TEXT,
              parent_title    TEXT,
              rating          INTEGER,
              revisions       INTEGER,
              children        INTEGER, -- Count of child pages
              comments        INTEGER, -- Count of comments
              commented_at    TEXT, -- ISO 8601 format recommended
              commented_by    TEXT,
              content         TEXT, -- Raw Wikidot source
              html            TEXT, -- Rendered HTML
              deleted_at      TEXT DEFAULT NULL, -- Added for tracking deletions
              -- Composite primary key ensures uniqueness per site
              PRIMARY KEY (site, fullname)
            )
            """
        )

        # Check and add deleted_at column if it doesn't exist (for backward compatibility)
        cursor.execute("PRAGMA table_info('pages');")
        columns = [column[1] for column in cursor.fetchall()]
        if 'deleted_at' not in columns:
            logger.info("Adding 'deleted_at' column to 'pages' table...")
            cursor.execute("ALTER TABLE pages ADD COLUMN deleted_at TEXT DEFAULT NULL;")
            logger.info("'deleted_at' column added.")


        # Separate table for tags (many-to-many relationship)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS page_tags (
              site     TEXT NOT NULL,
              fullname TEXT NOT NULL,
              tag      TEXT NOT NULL,
              -- Composite primary key ensures uniqueness
              PRIMARY KEY (site, fullname, tag),
              -- Foreign key constraint (optional but good practice)
              FOREIGN KEY (site, fullname) REFERENCES pages (site, fullname)
                  ON DELETE CASCADE ON UPDATE CASCADE
            )
            """
        )

        # Category information table (from categories.select)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
              site         TEXT NOT NULL,
              category     TEXT NOT NULL,
              PRIMARY KEY (site, category)
            )
            """
        )

        # File metadata table (from files.get_meta and files.select)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
              site             TEXT NOT NULL,
              page_fullname    TEXT NOT NULL,
              file_name        TEXT NOT NULL,
              size             INTEGER,
              comment          TEXT,
              mime_type        TEXT,
              mime_description TEXT,
              uploaded_by      TEXT,
              uploaded_at      TEXT,
              download_url     TEXT,
              PRIMARY KEY (site, page_fullname, file_name),
              FOREIGN KEY (site, page_fullname) REFERENCES pages (site, fullname)
                  ON DELETE CASCADE ON UPDATE CASCADE
            )
            """
        )

        # Comments/Posts table (from posts.select and posts.get)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
              site           TEXT NOT NULL,
              post_id        TEXT NOT NULL,
              page_fullname  TEXT NOT NULL,
              reply_to       TEXT,
              title          TEXT,
              content        TEXT,
              html           TEXT,
              created_by     TEXT,
              created_at     TEXT,
              replies        INTEGER,
              PRIMARY KEY (site, post_id),
              FOREIGN KEY (site, page_fullname) REFERENCES pages (site, fullname)
                  ON DELETE CASCADE ON UPDATE CASCADE
            )
            """
        )

        # User information table (from users.get_me and other endpoints)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              user_id      TEXT NOT NULL,
              name         TEXT NOT NULL,
              title        TEXT,
              PRIMARY KEY (user_id)
            )
            """
        )

        # Consider adding indexes for faster lookups if needed
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_page_tags_tag ON page_tags (tag);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_page ON files (site, page_fullname);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_page ON posts (site, page_fullname);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pages_deleted ON pages (deleted_at);") # Index for deleted_at

        conn.commit()
        logger.info("Database tables checked/created successfully.")
    except sqlite3.Error as e:
        logger.error("Database error during table creation: %s", e)
        raise  # Reraise after logging


def insert_page(conn: sqlite3.Connection, site: str, page_data: Dict[str, Any]) -> None:
    """
    Inserts or replaces a single page's data into the 'pages' table.
    Sets deleted_at to NULL on insert/replace to mark it as active.
    """
    logger.debug(
        "Inserting/Replacing page data for '%s' on site '%s'",
        page_data.get("fullname"),
        site,
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR REPLACE INTO pages (
              site, fullname, title, created_at, created_by, updated_at, updated_by,
              parent_fullname, parent_title, rating, revisions, children, comments,
              commented_at, commented_by, content, html, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL) -- Set deleted_at to NULL
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
        # No need to commit here, handled after tags or in main loop chunk
    except sqlite3.Error as e:
        logger.error(
            "DB error inserting page '%s' on site '%s': %s",
            page_data.get("fullname"),
            site,
            e,
        )
        conn.rollback()
        raise


def insert_tags(
    conn: sqlite3.Connection,
    site: str,
    page_fullname: str,
    tags_list: Optional[List[str]],  # Accept None
) -> None:
    """
    Inserts or replaces the tags for a single page in the 'page_tags' table.
    Deletes existing tags for the page before inserting new ones.
    """
    if tags_list is None:
        tags_list = []  # Ensure it's an iterable

    logger.debug("Updating tags for page '%s' on site '%s'", page_fullname, site)
    cursor = conn.cursor()
    try:
        # Delete existing tags for this page first
        cursor.execute(
            "DELETE FROM page_tags WHERE site = ? AND fullname = ?",
            (site, page_fullname),
        )
        # Insert new tags if any exist
        if tags_list:
            tag_data = [(site, page_fullname, tag) for tag in tags_list]
            cursor.executemany(
                "INSERT OR REPLACE INTO page_tags (site, fullname, tag) VALUES (?, ?, ?)",
                tag_data,
            )
        # No need to commit here, handled after page insert or in main loop chunk
    except sqlite3.Error as e:
        logger.error(
            "DB error updating tags for page '%s' on site '%s': %s",
            page_fullname,
            site,
            e,
        )
        conn.rollback()
        raise


def insert_categories(conn: sqlite3.Connection, site: str, categories: List[str]) -> None:
    """
    Inserts or replaces categories for a site in the 'categories' table.

    Args:
        conn: SQLite database connection.
        site: The Wikidot site name.
        categories: List of category names.
    """
    logger.debug("Updating categories for site '%s'", site)
    cursor = conn.cursor()
    try:
        # Delete existing categories for this site first
        cursor.execute("DELETE FROM categories WHERE site = ?", (site,))
        # Insert new categories if any exist
        if categories:
            category_data = [(site, category) for category in categories]
            cursor.executemany(
                "INSERT OR REPLACE INTO categories (site, category) VALUES (?, ?)",
                category_data,
            )
    except sqlite3.Error as e:
        logger.error(
            "DB error updating categories for site '%s': %s",
            site,
            e,
        )
        conn.rollback()
        raise


def insert_files(
    conn: sqlite3.Connection, site: str, page_fullname: str, files_data: Dict[str, Dict[str, Any]]
) -> None:
    """
    Inserts or replaces file metadata for a page in the 'files' table.

    Args:
        conn: SQLite database connection.
        site: The Wikidot site name.
        page_fullname: The full page name the files are attached to.
        files_data: Dictionary from files.get_meta with file metadata.
    """
    logger.debug("Updating file metadata for page '%s' on site '%s'", page_fullname, site)
    cursor = conn.cursor()
    try:
        # Delete existing file metadata for this page first
        cursor.execute(
            "DELETE FROM files WHERE site = ? AND page_fullname = ?",
            (site, page_fullname)
        )

        # Insert new file metadata
        if files_data:
            file_records = []
            for file_name, file_info in files_data.items():
                file_records.append((
                    site,
                    page_fullname,
                    file_name,
                    file_info.get("size"),
                    file_info.get("comment"),
                    file_info.get("mime_type"),
                    file_info.get("mime_description"),
                    file_info.get("uploaded_by"),
                    file_info.get("uploaded_at"),
                    file_info.get("download_url")
                ))

            cursor.executemany(
                """
                INSERT OR REPLACE INTO files (
                    site, page_fullname, file_name, size, comment, mime_type,
                    mime_description, uploaded_by, uploaded_at, download_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                file_records
            )
    except sqlite3.Error as e:
        logger.error(
            "DB error updating file metadata for page '%s' on site '%s': %s",
            page_fullname,
            site,
            e,
        )
        conn.rollback()
        raise


def insert_comments(
    conn: sqlite3.Connection, site: str, page_fullname: str, comments_data: Dict[str, Dict[str, Any]]
) -> None:
    """
    Inserts or replaces comment data for a page in the 'posts' table.

    Args:
        conn: SQLite database connection.
        site: The Wikidot site name.
        page_fullname: The full page name the comments belong to.
        comments_data: Dictionary from posts.get with comment data.
    """
    logger.debug("Updating comments for page '%s' on site '%s'", page_fullname, site)
    cursor = conn.cursor()
    try:
        # Delete existing comments for this page first
        cursor.execute(
            "DELETE FROM posts WHERE site = ? AND page_fullname = ?",
            (site, page_fullname)
        )

        # Insert new comments
        if comments_data:
            comment_records = []
            for post_id, comment_info in comments_data.items():
                comment_records.append((
                    site,
                    post_id,
                    page_fullname,
                    comment_info.get("reply_to"),
                    comment_info.get("title"),
                    comment_info.get("content"),
                    comment_info.get("html"),
                    comment_info.get("created_by"),
                    comment_info.get("created_at"),
                    comment_info.get("replies", 0)
                ))

            cursor.executemany(
                """
                INSERT OR REPLACE INTO posts (
                    site, post_id, page_fullname, reply_to, title, content,
                    html, created_by, created_at, replies
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                comment_records
            )
    except sqlite3.Error as e:
        logger.error(
            "DB error updating comments for page '%s' on site '%s': %s",
            page_fullname,
            site,
            e,
        )
        conn.rollback()
        raise


def get_db_page_info(
    conn: sqlite3.Connection, site: str, page_name: str
) -> Optional[Tuple[Optional[str], Optional[int], Optional[str]]]: # Added deleted_at
    """
    Retrieves the stored updated_at, revisions, and deleted_at for a page from the DB.

    Returns:
        A tuple (updated_at, revisions, deleted_at) if the page exists, otherwise None.
        Values within the tuple can be None if not set in DB.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT updated_at, revisions, deleted_at FROM pages WHERE site=? AND fullname=?",
            (site, page_name),
        )
        row = cursor.fetchone()
        # row is tuple (updated_at, revisions, deleted_at) or None
        return row  # type: ignore
    except sqlite3.Error as e:
        logger.error(
            "DB error fetching info for page '%s' on site '%s': %s", page_name, site, e
        )
        return None  # Treat DB error as 'not found' for simplicity here


# --- Main Processing Logic ---


def process_site_additional_data(
    conn: sqlite3.Connection,
    server: xmlrpc.client.ServerProxy,
    site: str
) -> None:
    """
    Fetches and stores additional site-wide data like categories.

    Args:
        conn: SQLite database connection.
        server: The authenticated ServerProxy instance.
        site: The Wikidot site name.
    """
    logger.info("Fetching additional site-wide data for site '%s'", site)

    # Fetch and store category information
    try:
        categories = fetch_all_categories(site, server)
        logger.info("Found %d categories for site '%s'", len(categories), site)
        insert_categories(conn, site, categories)
        conn.commit()
        logger.info("Successfully stored categories for site '%s'", site)
    except Exception as e:
        logger.error("Error fetching or storing categories for site '%s': %s", site, e)
        conn.rollback()


def process_page_additional_data(
    conn: sqlite3.Connection,
    server: xmlrpc.client.ServerProxy,
    site: str,
    page_name: str
) -> None:
    """
    Fetches and stores additional data for a specific page (files, comments).

    Args:
        conn: SQLite database connection.
        server: The authenticated ServerProxy instance.
        site: The Wikidot site name.
        page_name: The full page name to process.
    """
    logger.debug("Processing additional data for page '%s' on site '%s'", page_name, site)

    # 1. Process file metadata
    try:
        file_names = fetch_page_files(site, server, page_name)
        if file_names:
            logger.debug("Found %d files for page '%s'", len(file_names), page_name)

            # Process files in chunks of 10 (API limitation)
            for i in range(0, len(file_names), 10):
                file_chunk = file_names[i:i+10]
                files_meta = fetch_files_meta(site, server, page_name, file_chunk)
                insert_files(conn, site, page_name, files_meta)
                conn.commit()

            logger.debug("Successfully stored file metadata for page '%s'", page_name)
    except Exception as e:
        logger.error("Error processing file metadata for page '%s': %s", page_name, e)
        conn.rollback()

    # 2. Process comments/posts
    try:
        comment_ids = fetch_page_comments(site, server, page_name)
        if comment_ids:
            logger.debug("Found %d comments for page '%s'", len(comment_ids), page_name)

            # Process comments in chunks of 10 (API limitation)
            for i in range(0, len(comment_ids), 10):
                comment_chunk = comment_ids[i:i+10]
                comments_data = fetch_comments_data(site, server, comment_chunk)
                insert_comments(conn, site, page_name, comments_data)
                conn.commit()

            logger.debug("Successfully stored comments for page '%s'", page_name)
    except Exception as e:
        logger.error("Error processing comments for page '%s': %s", page_name, e)
        conn.rollback()


def process_single_page(
    conn: sqlite3.Connection,
    server: xmlrpc.client.ServerProxy,
    site: str,
    page_name: str,
    meta: Dict[str, Any],
) -> bool:
    """
    Processes a single page: checks if update needed, fetches full data if so,
    and updates the database. Handles marking pages as deleted.

    Args:
        conn: SQLite database connection.
        server: Authenticated ServerProxy instance.
        site: The Wikidot site name.
        page_name: The fullname of the page to process.
        meta: The metadata dictionary for this page from get_pages_meta.

    Returns:
        True if the page was processed (fetched and/or updated), False if skipped or failed.
    """
    # 1. Check if update is needed by comparing with DB
    db_info = get_db_page_info(conn, site, page_name)
    if db_info is not None:
        db_updated_at, db_revisions, db_deleted_at = db_info # Unpack deleted_at
        meta_updated_at = meta.get("updated_at")
        meta_revisions = meta.get("revisions")

        # If page exists in DB and is marked as deleted, but now exists in meta,
        # it means it was recreated. We need to update it.
        if db_deleted_at is not None:
             logger.info("Page '%s' on site '%s' was previously marked deleted, now exists. Re-fetching.", page_name, site)
        # If page exists in DB (and is not marked deleted) and metadata matches, skip.
        elif db_updated_at == meta_updated_at and db_revisions == meta_revisions:
            logger.debug(
                "Skipping '%s' on site '%s': No changes detected.", page_name, site
            )
            return False  # Skipped (no change)

    logger.debug(
        "Processing page '%s' on site '%s': Update needed or new page.", page_name, site
    )

    # 2. Fetch full page data if update is needed or page is new
    fullinfo: Optional[Dict[str, Any]] = None # Initialize fullinfo
    try:
        fullinfo = get_one_page(site, server, page_name)

    except xmlrpc.client.Fault as fault:
        # Handle specific "page does not exist" error (406) after retries
        if (
            fault.faultCode == 406
            # Check common variations of the "page does not exist" message
            and (
                "page does not exist" in fault.faultString.lower()
                or "page not found" in fault.faultString.lower()
            )
        ):
            logger.warning(
                "Page '%s' on site '%s' does not exist (406). Marking as deleted in DB.",
                page_name,
                site,
            )
            try:
                # Mark the page as deleted in the database if it's not already marked
                deleted_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE pages
                    SET deleted_at = ?
                    WHERE site = ? AND fullname = ? AND deleted_at IS NULL
                    """,
                    (deleted_timestamp, site, page_name),
                )
                if cursor.rowcount > 0: # Check if any row was actually updated
                    conn.commit()
                    logger.info("Marked page '%s' on site '%s' as deleted at %s.",
                                page_name, site, deleted_timestamp)
                else:
                    # Page might not exist in DB or was already marked deleted
                    logger.debug("Page '%s' on site '%s' not found in DB or already marked deleted. No update needed.", page_name, site)
                    conn.rollback() # Rollback if no rows were updated
            except sqlite3.Error as db_err:
                logger.error(
                    "Failed to mark page '%s' on site '%s' as deleted in DB: %s",
                    page_name, # Use function argument page_name
                    site,
                    db_err,
                    exc_info=True,
                )
                conn.rollback() # Rollback on error
            return False  # Skipped due to deletion/error during marking
        else:
            # Log other API faults encountered during get_one_page
            logger.error(
                "API Fault after retries fetching page '%s' on site '%s': %s", page_name, site, fault
            )
            # Consider if specific non-403/406 faults should also lead to skipping
            return False # Treat other persistent faults as failure for this page

    except RETRYABLE_EXCEPTIONS as e:
        # Catch errors that might occur if get_one_page retries ultimately failed
        logger.error(
            "Network/API error after all retries fetching page '%s' on site '%s': %s",
            page_name,
            site,
            e,
        )
        return False  # Failed to process

    except Exception as e:
        # Catch any other unexpected errors during get_one_page
        logger.exception(
            "Unexpected error fetching page '%s' on site '%s': %s",
            page_name,
            site,
            e,
            exc_info=True,
        )  # Use logger.exception for stack trace
        return False  # Failed to process

    # Check if get_one_page returned None (due to 403 error handled within it)
    if fullinfo is None:
        logger.info(
            "Skipping DB update for page '%s' on site '%s' due to 403 error during fetch.",
            page_name, site
        )
        return False # Indicate page was not processed successfully

    # Add tags from metadata (get_one doesn't always include them reliably)
    # We know fullinfo is a Dict here because we checked for None above
    fullinfo["tags"] = meta.get("tags", [])

    # 3. Insert/Update page and tags in the database
    try:
        # Use a transaction for inserting page and its tags
        with conn: # Context manager handles commit/rollback on exit
            # We know fullinfo is a Dict here
            insert_page(conn, site, fullinfo)
            # We know fullinfo is a Dict here, .get() is safe
            insert_tags(conn, site, fullinfo['fullname'], fullinfo.get("tags", [])) # Use fullinfo['fullname'] for consistency
        # Commit happens automatically when 'with conn:' block exits without error

        logger.debug(
            "Successfully updated page '%s' and its tags in DB on site '%s'.",
            page_name, site
        )

        # 4. Process additional data (files, comments) only after successful page/tag update
        try:
            process_page_additional_data(conn, server, site, page_name) # Pass server proxy
            return True # Processed successfully including additional data

        except Exception as add_data_err:
            # Log error during additional data processing but consider page update successful
            logger.exception(
                "Error processing additional data (files/comments) for page '%s' on site '%s' after DB update: %s",
                page_name, site, add_data_err
            )
            # Return True because the main page data was updated, but log the issue.
            # Alternatively, return False if additional data failure should mark the whole process as failed.
            return True # Or False, depending on desired behavior

    except sqlite3.Error as db_err:
        logger.error(
            "Database error during transaction for page '%s' on site '%s': %s",
            page_name, site, db_err
        )
        # Rollback is automatically handled by the 'with conn:' context manager on exception
        return False # Failed to process page/tags

    except Exception as e:
        # Catch unexpected errors during the DB transaction itself
        logger.exception(
            "Unexpected error during DB transaction for page '%s' on site '%s': %s",
            page_name, site, e
        )
        # Rollback is automatically handled by the 'with conn:' context manager on exception
        return False # Failed to process page/tags


def process_site(
    conn: sqlite3.Connection,
    server: xmlrpc.client.ServerProxy,
    site: str,
) -> None:
    """
    Processes a single site, fetching and updating all its pages and related data.

    Args:
        conn: SQLite database connection.
        server: The authenticated ServerProxy instance.
        site: The Wikidot site name to process.
    """
    # Start measuring site processing time
    site_start_time = time.time()
    logger.info("=== Processing site: %s ===", site)
    all_pages: List[str] = []
    try:
        all_pages = select_all_pages(site, server)
        total_pages = len(all_pages)
        logger.info("Found %d pages for site '%s'.", total_pages, site)
        if total_pages == 0:
            logger.warning("No pages found for site '%s'. Skipping.", site)
            return
    except Exception as e:
        # Catch errors from select_all_pages if retries fail
        logger.error(
            "Failed to retrieve page list for site '%s' after retries: %s", site, e
        )
        logger.warning(
            "Skipping site '%s' due to page list retrieval failure.", site
        )
        return  # Skip to the next site

    # Process additional site-wide data (categories)
    process_site_additional_data(conn, server, site)

    processed_count = 0
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    # Process pages in chunks
    for i in range(0, total_pages, CHUNK_SIZE):
        chunk_start_time = time.time()  # Start measuring chunk processing time
        chunk = all_pages[i : i + CHUNK_SIZE]
        logger.info(
            "Processing chunk %d/%d for site '%s' (%d pages)",
            (i // CHUNK_SIZE) + 1,
            (total_pages + CHUNK_SIZE - 1) // CHUNK_SIZE,
            site,
            len(chunk),
        )

        # Get metadata for the current chunk
        meta_info: Dict[str, Dict[str, Any]] = {}
        try:
            meta_info = get_pages_meta(site, server, chunk)
        except Exception as e:
            # Catch errors from get_pages_meta if retries fail
            logger.error(
                "Failed to get metadata for chunk on site '%s' after retries: %s",
                site,
                e,
            )
            logger.warning(
                "Skipping this chunk (%d pages) for site '%s'.", len(chunk), site
            )
            failed_count += len(chunk)  # Count all in chunk as failed
            processed_count += len(chunk)
            continue  # Skip this chunk

        # Process each page within the chunk using metadata
        for page_name in chunk:
            processed_count += 1
            meta = meta_info.get(page_name)

            if not meta:
                logger.warning(
                    "Metadata missing for page '%s' in chunk on site '%s'. Skipping.",
                    page_name,
                    site,
                )
                failed_count += 1
                continue  # Skip page if no metadata retrieved

            # Call the helper function to process this single page
            try:
                success = process_single_page(conn, server, site, page_name, meta)
                if success:
                    updated_count += 1
                else:
                    # Skipped (no change, deleted, or failed fetch/DB update)
                    skipped_count += 1  # Count includes no-change, deleted, failed
            except Exception as e:
                # Catch unexpected errors from process_single_page itself (should be rare)
                logger.exception(
                    "Unexpected error processing page '%s' on site '%s' in main loop: %s",
                    page_name,
                    site,
                    e,
                    exc_info=True,
                )
                failed_count += 1

            # Log progress periodically
            if processed_count % 50 == 0 or processed_count == total_pages:
                logger.info(
                    "Site '%s': Processed %d/%d pages...",
                    site,
                    processed_count,
                    total_pages,
                )

        # Calculate and log chunk processing time
        chunk_elapsed = time.time() - chunk_start_time
        pages_per_second = len(chunk) / chunk_elapsed if chunk_elapsed > 0 else 0
        logger.info(
            "Chunk processed in %.2f seconds (%.2f pages/sec)",
            chunk_elapsed,
            pages_per_second,
        )

    # Calculate and log site processing time
    site_elapsed = time.time() - site_start_time
    pages_per_second = (
        total_pages / site_elapsed if site_elapsed > 0 and total_pages > 0 else 0
    )

    logger.info("=== Finished site: %s ===", site)
    logger.info("  Total pages checked: %d", processed_count)
    logger.info("  Pages updated/inserted: %d", updated_count)
    logger.info(
        "  Pages skipped (no change/deleted/failed): %d",
        skipped_count, # Adjusted to not double-count failures
    )
    logger.info( # Added separate line for failures
        "  Pages failed processing: %d",
        failed_count
    )
    logger.info(
        "  Site processing time: %.2f seconds (%.2f pages/sec)",
        site_elapsed,
        pages_per_second,
    )


def main() -> None:
    """
    Main execution function:
    1. Connects to the database and ensures tables exist.
    2. Connects to the Wikidot API.
    3. Iterates through each site defined in SITES.
    4. Processes each site including pages and additional data.
    5. Logs progress and completion.
    """
    # Start measuring total execution time
    start_time_total = time.time()
    logger.info("Starting Wikidot data synchronization...")
    logger.info("Database file: %s", DB_FILE)

    # Ensure data directory exists if DB_FILE includes a path
    db_dir = os.path.dirname(DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir)
            logger.info("Created database directory: %s", db_dir)
        except OSError as e:
            logger.error("Failed to create database directory '%s': %s", db_dir, e)
            return  # Cannot proceed without DB directory

    # 1. Connect to Database
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)  # Added timeout
        # Improve performance and reduce locking issues
        conn.execute("PRAGMA journal_mode=WAL;")
        create_tables(conn) # Ensures deleted_at column exists
    except sqlite3.Error as e:
        logger.error("Failed to connect to or initialize database '%s': %s", DB_FILE, e)
        return  # Cannot proceed without DB

    # 2. Connect to Wikidot API
    server: Optional[xmlrpc.client.ServerProxy] = None
    try:
        server = get_server_proxy(API_USER, API_KEY)
        # Optionally test connection further here if needed
        # server.system.listMethods()
        logger.info("Successfully connected to Wikidot API.")
    except Exception as e:
        # Catch exceptions from get_server_proxy if retries fail
        logger.error("Failed to connect to Wikidot API after multiple retries: %s", e)
        logger.info("Check API credentials and network connection. Exiting.")
        if conn:
            conn.close()
        return

    # --- Site Processing Loop ---
    for site in SITES:
        try:
            process_site(conn, server, site)
        except Exception as e:
            logger.error("Error processing site '%s': %s", site, e, exc_info=True) # Added exc_info
            logger.warning("Continuing with next site...")

    # --- Cleanup ---
    if conn:
        try:
            conn.close()
            logger.info("Database connection closed.")
        except sqlite3.Error as e:
            logger.error("Error closing database connection: %s", e)

    # Calculate and log total execution time
    total_elapsed = time.time() - start_time_total
    logger.info("Synchronization complete for all sites.")
    logger.info(
        "Total execution time: %.2f seconds (%.2f minutes)",
        total_elapsed,
        total_elapsed / 60.0,
    )


if __name__ == "__main__":
    # Basic check for required environment variables
    if API_USER == "your-username" or API_KEY == "your-api-key":
        logger.error(
            "API_USER or API_KEY not set in environment variables or .env file."
        )
        logger.error(
            "Exiting program as API calls cannot function without proper credentials."
        )
        exit("Error: Wikidot credentials not configured.")
    main() # Call main function
