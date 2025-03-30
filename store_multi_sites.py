"""
store_multi_sites.py

Fetches pages from multiple Wikidot sites (EN, JP, CN, KO, etc.)
using the Wikidot API and stores them in an SQLite database.
Uses (site, fullname) as the primary key to differentiate pages
from different sites even if they share the same fullname.
Implements differential updates based on metadata to improve efficiency.
"""

import os
import sqlite3
import xmlrpc.client
import logging
import socket
from typing import Any, Dict, List, Optional, Tuple, cast
import time

# Security patch for xmlrpc
import defusedxml.xmlrpc
from dotenv import load_dotenv
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
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
    "scp-wiki",      # English (main)
    "scp-jp",        # Japanese Branch
    "scp-wiki-cn",   # Chinese Branch
    "scpko",         # Korean Branch
    # Add more sites here as needed
]

# Chunk size for batch API calls (pages.select, get_meta)
CHUNK_SIZE = 10

# Retry settings for tenacity
MAX_RETRY_ATTEMPTS = 10
MAX_WAIT_TIME = 900

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
    ConnectionError,      # Handles network connection issues
    socket.gaierror,      # Handles DNS resolution errors
    socket.timeout,       # Handles socket timeouts
    xmlrpc.client.ProtocolError, # Handles protocol errors (e.g., bad gateway)
)

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True, # Reraise the exception if all retries fail
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
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
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
    return cast(List[str], server.pages.select({"site": site}))

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
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
        Dict[str, Dict[str, Any]],
        server.pages.get_meta({"site": site, "pages": pages})
    )

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, max=MAX_WAIT_TIME),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True,
)
def get_one_page(
    site: str, server: xmlrpc.client.ServerProxy, page_name: str
) -> Dict[str, Any]:
    """
    Fetches the full content and details for a single page.

    Retries on common network and API errors.

    Args:
        site: The Wikidot site name.
        server: The authenticated ServerProxy instance.
        page_name: The fullname of the page to fetch.

    Returns:
        A dictionary containing page details (content, html, rating, etc.).

    Raises:
        Catches and retries RETRYABLE_EXCEPTIONS. Reraises if retries fail.
        Note: Specific Faults like 406 (page not found) might be handled
              by the caller after this function returns/raises.
    """
    logger.debug("Fetching full content for page: %s on site %s", page_name, site)
    return cast(Dict[str, Any], server.pages.get_one({"site": site, "page": page_name}))


# --- Database Operations ---

def create_tables(conn: sqlite3.Connection) -> None:
    """
    Creates the necessary tables (pages, page_tags) in the SQLite database
    if they don't already exist. Uses (site, fullname) as composite keys.
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
              -- Composite primary key ensures uniqueness per site
              PRIMARY KEY (site, fullname)
            )
            """
        )

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
        # Consider adding indexes for faster lookups if needed
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_page_tags_tag ON page_tags (tag);")
        conn.commit()
        logger.info("Database tables checked/created successfully.")
    except sqlite3.Error as e:
        logger.error("Database error during table creation: %s", e)
        raise # Reraise after logging


def insert_page(conn: sqlite3.Connection, site: str, page_data: Dict[str, Any]) -> None:
    """
    Inserts or replaces a single page's data into the 'pages' table.
    """
    logger.debug("Inserting/Replacing page data for '%s' on site '%s'", page_data.get('fullname'), site)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR REPLACE INTO pages (
              site, fullname, title, created_at, created_by, updated_at, updated_by,
              parent_fullname, parent_title, rating, revisions, children, comments,
              commented_at, commented_by, content, html
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
        # No need to commit here, handled after tags or in main loop chunk
    except sqlite3.Error as e:
        logger.error("DB error inserting page '%s' on site '%s': %s",
                     page_data.get('fullname'), site, e)
        # Decide whether to raise or just log and continue
        # raise

def insert_tags(
    conn: sqlite3.Connection,
    site: str,
    page_fullname: str,
    tags_list: Optional[List[str]], # Accept None
) -> None:
    """
    Inserts or replaces the tags for a single page in the 'page_tags' table.
    Deletes existing tags for the page before inserting new ones.
    """
    if tags_list is None:
        tags_list = [] # Ensure it's an iterable

    logger.debug("Updating tags for page '%s' on site '%s'", page_fullname, site)
    cursor = conn.cursor()
    try:
        # Delete existing tags for this page first
        cursor.execute(
            "DELETE FROM page_tags WHERE site = ? AND fullname = ?",
            (site, page_fullname)
        )
        # Insert new tags if any exist
        if tags_list:
            tag_data = [(site, page_fullname, tag) for tag in tags_list]
            cursor.executemany(
                "INSERT OR REPLACE INTO page_tags (site, fullname, tag) VALUES (?, ?, ?)",
                tag_data
            )
        # No need to commit here, handled after page insert or in main loop chunk
    except sqlite3.Error as e:
        logger.error("DB error updating tags for page '%s' on site '%s': %s",
                     page_fullname, site, e)
        # Decide whether to raise or just log and continue
        # raise


def get_db_page_info(
    conn: sqlite3.Connection, site: str, page_name: str
) -> Optional[Tuple[Optional[str], Optional[int]]]:
    """
    Retrieves the stored updated_at and revisions count for a page from the DB.

    Returns:
        A tuple (updated_at, revisions) if the page exists, otherwise None.
        Values within the tuple can be None if not set in DB.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT updated_at, revisions FROM pages WHERE site=? AND fullname=?",
            (site, page_name),
        )
        row = cursor.fetchone()
        # row is tuple (updated_at, revisions) or None
        return row # type: ignore
    except sqlite3.Error as e:
        logger.error("DB error fetching info for page '%s' on site '%s': %s",
                     page_name, site, e)
        return None # Treat DB error as 'not found' for simplicity here


# --- Main Processing Logic ---

def process_single_page(
    conn: sqlite3.Connection,
    server: xmlrpc.client.ServerProxy,
    site: str,
    page_name: str,
    meta: Dict[str, Any]
) -> bool:
    """
    Processes a single page: checks if update needed, fetches full data if so,
    and updates the database.

    Args:
        conn: SQLite database connection.
        server: Authenticated ServerProxy instance.
        site: The Wikidot site name.
        page_name: The fullname of the page to process.
        meta: The metadata dictionary for this page from get_pages_meta.

    Returns:
        True if the page was processed (fetched and/or updated), False if skipped.
    """
    # 1. Check if update is needed by comparing with DB
    db_info = get_db_page_info(conn, site, page_name)
    if db_info is not None:
        db_updated_at, db_revisions = db_info
        meta_updated_at = meta.get("updated_at")
        meta_revisions = meta.get("revisions") # Returns None if key missing
        # Ensure comparison handles None correctly, treat None revision as 0?
        # Wikidot API might return 0 or omit revisions key, DB might store NULL.
        # Simple comparison: skip only if both match exactly.
        if db_updated_at == meta_updated_at and db_revisions == meta_revisions:
            logger.debug("Skipping '%s' on site '%s': No changes detected.", page_name, site)
            return False # Skipped

    logger.debug("Processing page '%s' on site '%s': Update needed or new page.", page_name, site)

    # 2. Fetch full page data if update is needed or page is new
    try:
        fullinfo = get_one_page(site, server, page_name)
        # Add tags from metadata (get_one doesn't always include them reliably)
        fullinfo["tags"] = meta.get("tags", [])

    except xmlrpc.client.Fault as fault:
        # Handle specific "page does not exist" error (406)
        if fault.faultCode == 406 and "page does not exist" in fault.faultString.lower():
            logger.warning("Page '%s' on site '%s' appears deleted (Fault 406). Skipping.",
                         page_name, site)
            # Optional: Mark page as deleted in DB instead of skipping?
            # delete_page_record(conn, site, page_name)
            return False # Skipped due to deletion
        else:
            # Log other API faults encountered during get_one_page
            # These might occur even after tenacity retries if it's not a retryable fault type
            logger.error("API Fault fetching page '%s' on site '%s': %s",
                         page_name, site, fault)
            return False # Failed to process

    except RETRYABLE_EXCEPTIONS as e:
        # Catch errors that might occur if get_one_page retries failed
        logger.error("Network/API error after retries fetching page '%s' on site '%s': %s",
                     page_name, site, e)
        return False # Failed to process

    except Exception as e:
        # Catch any other unexpected errors during get_one_page
        logger.exception("Unexpected error fetching page '%s' on site '%s': %s",
                         page_name, site, e, exc_info=True) # Use logger.exception for stack trace
        return False # Failed to process

    # 3. Insert/Update page and tags in the database
    try:
        # Use a transaction for inserting page and its tags
        with conn: # Context manager handles commit/rollback
            insert_page(conn, site, fullinfo)
            insert_tags(conn, site, page_name, fullinfo.get("tags"))
        logger.debug("Successfully updated page '%s' and its tags in DB.", page_name, site)
        return True # Successfully processed

    except sqlite3.Error as e:
        logger.error("DB error updating page '%s' or tags on site '%s': %s",
                     page_name, site, e)
        # Transaction should automatically rollback on exception with 'with conn:'
        return False # Failed to process
    except Exception as e:
        # Catch unexpected errors during DB operation
        logger.exception("Unexpected error updating DB for page '%s' on site '%s': %s",
                         page_name, site, e, exc_info=True)
        return False # Failed to process


def main() -> None:
    """
    Main execution function:
    1. Connects to the database and ensures tables exist.
    2. Connects to the Wikidot API.
    3. Iterates through each site defined in SITES.
    4. Fetches all page names for the site.
    5. Processes pages in chunks:
       a. Fetches metadata for the chunk.
       b. For each page in the chunk, calls process_single_page.
    6. Logs progress and completion.
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
            return # Cannot proceed without DB directory

    # 1. Connect to Database
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10) # Added timeout
        # Improve performance and reduce locking issues
        conn.execute("PRAGMA journal_mode=WAL;")
        create_tables(conn)
    except sqlite3.Error as e:
        logger.error("Failed to connect to or initialize database '%s': %s", DB_FILE, e)
        return # Cannot proceed without DB

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
                continue
        except Exception as e:
            # Catch errors from select_all_pages if retries fail
            logger.error("Failed to retrieve page list for site '%s' after retries: %s", site, e)
            logger.warning("Skipping site '%s' due to page list retrieval failure.", site)
            continue # Skip to the next site

        processed_count = 0
        updated_count = 0
        skipped_count = 0
        failed_count = 0

        # Process pages in chunks
        for i in range(0, total_pages, CHUNK_SIZE):
            chunk_start_time = time.time()  # Start measuring chunk processing time
            chunk = all_pages[i : i + CHUNK_SIZE]
            logger.info("Processing chunk %d/%d for site '%s' (%d pages)",
                         (i // CHUNK_SIZE) + 1,
                         (total_pages + CHUNK_SIZE - 1) // CHUNK_SIZE,
                         site, len(chunk))

            # Get metadata for the current chunk
            meta_info: Dict[str, Dict[str, Any]] = {}
            try:
                meta_info = get_pages_meta(site, server, chunk)
            except Exception as e:
                # Catch errors from get_pages_meta if retries fail
                logger.error("Failed to get metadata for chunk on site '%s' after retries: %s", site, e)
                logger.warning("Skipping this chunk (%d pages) for site '%s'.", len(chunk), site)
                failed_count += len(chunk) # Count all in chunk as failed
                processed_count += len(chunk)
                continue # Skip this chunk

            # Process each page within the chunk using metadata
            for page_name in chunk:
                processed_count += 1
                meta = meta_info.get(page_name)

                if not meta:
                    logger.warning("Metadata missing for page '%s' in chunk on site '%s'. Skipping.",
                                 page_name, site)
                    failed_count += 1
                    continue # Skip page if no metadata retrieved

                # Call the helper function to process this single page
                try:
                    success = process_single_page(conn, server, site, page_name, meta)
                    if success:
                        updated_count += 1
                    else:
                        # Skipped (no change, deleted, or failed fetch/DB update)
                        # process_single_page logs specifics, here we just count overall skips/fails
                        # We can't easily distinguish between skipped-no-change and skipped-due-to-error
                        # without more return values, but this simplifies main loop.
                        skipped_count += 1 # Count includes no-change, deleted, failed
                except Exception as e:
                    # Catch unexpected errors from process_single_page itself (should be rare)
                    logger.exception("Unexpected error processing page '%s' on site '%s' in main loop: %s",
                                     page_name, site, e, exc_info=True)
                    failed_count += 1


                # Log progress periodically
                if processed_count % 50 == 0 or processed_count == total_pages:
                    logger.info("Site '%s': Processed %d/%d pages...",
                                 site, processed_count, total_pages)

            # Calculate and log chunk processing time
            chunk_elapsed = time.time() - chunk_start_time
            pages_per_second = len(chunk) / chunk_elapsed if chunk_elapsed > 0 else 0
            logger.info("Chunk processed in %.2f seconds (%.2f pages/sec)", 
                        chunk_elapsed, pages_per_second)

            # Optional: Commit transactions periodically per chunk if not using 'with conn:' inside helper
            # try:
            #     conn.commit()
            # except sqlite3.Error as e:
            #     logger.error("DB commit error after chunk on site '%s': %s", site, e)

        # Calculate and log site processing time
        site_elapsed = time.time() - site_start_time
        pages_per_second = total_pages / site_elapsed if site_elapsed > 0 and total_pages > 0 else 0
        
        logger.info("=== Finished site: %s ===", site)
        logger.info("  Total pages checked: %d", processed_count)
        logger.info("  Pages updated/inserted: %d", updated_count)
        logger.info("  Pages skipped (no change/deleted/failed): %d", skipped_count + failed_count)
        logger.info("  Site processing time: %.2f seconds (%.2f pages/sec)", 
                    site_elapsed, pages_per_second)
        # Note: skipped_count implicitly includes failed_count based on current logic. Clarify if needed.

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
    logger.info("Total execution time: %.2f seconds (%.2f minutes)", 
                total_elapsed, total_elapsed / 60.0)


if __name__ == "__main__":
    # Basic check for required environment variables
    if API_USER == "your-username" or API_KEY == "your-api-key":
        logger.warning("API_USER or API_KEY not set in environment variables or .env file.")
        logger.warning("Using default placeholder values, API calls will likely fail.")
        # Consider exiting if credentials are required:
        # exit("Error: Wikidot credentials not configured.")

    main()