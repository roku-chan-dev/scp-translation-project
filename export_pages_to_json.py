import sqlite3
import json
import os
import logging
import re # Import re for sanitization
from typing import Dict, List, Any, Optional

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_FILE = os.getenv("DB_FILE", "data/scp_data.sqlite")
OUTPUT_DIR = os.getenv("JSON_OUTPUT_DIR", "data/pages")
# --- End Configuration ---

def sanitize_filename(filename: str) -> str:
    """Replaces characters invalid for Windows filenames/directory names with underscores."""
    # Invalid characters for Windows: < > : " / \ | ? *
    # Also replace space with underscore for better compatibility
    sanitized = re.sub(r'[<>:"/\\|?*\s]', '_', filename)
    # Remove leading/trailing dots and underscores
    sanitized = sanitized.strip('._')
    # Ensure the name is not empty after sanitization
    if not sanitized:
        return "_"
    # Limit length to avoid issues with long paths (optional, adjust as needed)
    # max_len = 100
    # if len(sanitized) > max_len:
    #     sanitized = sanitized[:max_len]
    return sanitized

def get_page_tags(conn: sqlite3.Connection, site: str, fullname: str) -> List[str]:
    """Fetches all tags for a specific page."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT tag FROM page_tags WHERE site = ? AND fullname = ?",
            (site, fullname),
        )
        tags = [row[0] for row in cursor.fetchall()]
        return tags
    except sqlite3.Error as e:
        logger.error(
            "DB error fetching tags for page '%s' on site '%s': %s", fullname, site, e
        )
        return []

def export_pages_to_json() -> None:
    """
    Exports active page data from the SQLite database to JSON files.
    Uses the directory structure: data/pages/[sanitized_fullname]/[site].json
    """
    if not os.path.exists(DB_FILE):
        logger.error(f"Database file not found at {DB_FILE}. Exiting.")
        return

    conn: Optional[sqlite3.Connection] = None
    exported_count = 0
    failed_count = 0

    try:
        conn = sqlite3.connect(DB_FILE)
        # Use dictionary cursor for easier access to columns by name
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        logger.info(f"Querying active pages from {DB_FILE}...")
        # Select only pages that are not marked as deleted
        cursor.execute(
            """
            SELECT
                site, fullname, title, content, created_by, created_at,
                updated_at, rating
            FROM pages
            WHERE deleted_at IS NULL
            ORDER BY site, fullname
            """
        )

        logger.info("Starting JSON export process...")
        if not os.path.exists(OUTPUT_DIR):
            try:
                os.makedirs(OUTPUT_DIR)
                logger.info(f"Created base output directory: {OUTPUT_DIR}")
            except OSError as e:
                logger.error(f"Failed to create base output directory '{OUTPUT_DIR}': {e}")
                return # Cannot proceed

        page_row = cursor.fetchone()
        while page_row:
            try:
                site = page_row["site"]
                fullname = page_row["fullname"]
                logger.debug(f"Processing page: {site}/{fullname}")

                # Sanitize the fullname before creating the directory path
                sanitized_fullname = sanitize_filename(fullname)
                page_output_dir = os.path.join(OUTPUT_DIR, sanitized_fullname)

                # Create subdirectory for the sanitized page slug if it doesn't exist
                if not os.path.exists(page_output_dir):
                    try:
                        os.makedirs(page_output_dir)
                        logger.debug(f"Created directory: {page_output_dir}")
                    except OSError as e:
                        # Log the specific error and the problematic path
                        logger.error(f"Failed to create directory '{page_output_dir}' (original fullname: '{fullname}'): {e}")
                        failed_count += 1
                        page_row = cursor.fetchone() # Move to next row
                        continue # Skip this page

                # Fetch tags for the page
                tags = get_page_tags(conn, site, fullname)

                # Prepare JSON data
                page_json_data: Dict[str, Any] = {
                    "title": page_row["title"],
                    "content": page_row["content"],
                    "tags": tags,
                    "author": page_row["created_by"], # Using created_by as author
                    "created_at": page_row["created_at"],
                    "updated_at": page_row["updated_at"],
                    "rating": page_row["rating"],
                    # Add site and fullname for potential cross-referencing if needed
                    "_site": site,
                    "_fullname": fullname,
                }

                # Define output file path: data/pages/[sanitized_fullname]/[site].json
                # Sanitize site name as well, just in case
                sanitized_site = sanitize_filename(site)
                output_file_path = os.path.join(page_output_dir, f"{sanitized_site}.json")

                # Write JSON data to file
                try:
                    with open(output_file_path, "w", encoding="utf-8") as f:
                        json.dump(page_json_data, f, ensure_ascii=False, indent=2)
                    logger.debug(f"Successfully wrote JSON to: {output_file_path}")
                    exported_count += 1
                except IOError as e:
                    logger.error(f"Failed to write JSON file '{output_file_path}': {e}")
                    failed_count += 1
                except json.JSONDecodeError as e:
                     logger.error(f"JSON encoding error for page '{site}/{fullname}': {e}")
                     failed_count += 1

            except Exception as e:
                # Catch unexpected errors during row processing
                page_info = dict(page_row) if page_row else 'N/A'
                logger.error(f"Unexpected error processing row: {page_info}. Error: {e}", exc_info=True)
                failed_count += 1

            # Fetch the next row
            page_row = cursor.fetchone()

    except sqlite3.Error as e:
        logger.error(f"Database error during export: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed.")

    logger.info("JSON export process finished.")
    logger.info(f"Successfully exported: {exported_count} pages.")
    logger.info(f"Failed to export: {failed_count} pages.")

if __name__ == "__main__":
    export_pages_to_json()