"""
Tests for database operations.
"""
import sqlite3
import pytest
from unittest.mock import patch

# Import functions from main script
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from store_multi_sites import (
    create_tables, insert_page, insert_tags, get_db_page_info
)


def test_create_tables(mock_db_connection):
    """Test creating database tables"""
    # Call function
    create_tables(mock_db_connection)
    
    # Verify tables were created
    cursor = mock_db_connection.cursor()
    
    # Check pages table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'")
    assert cursor.fetchone() is not None
    
    # Check page_tags table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='page_tags'")
    assert cursor.fetchone() is not None


def test_insert_page(mock_db_connection):
    """Test inserting a page into the database"""
    # Set up database
    create_tables(mock_db_connection)
    
    # Test data
    site = "scp-wiki"
    page_data = {
        "fullname": "scp-001",
        "title": "SCP-001 Test",
        "created_at": "2025-01-01T00:00:00",
        "created_by": "testuser",
        "updated_at": "2025-03-30T12:00:00",
        "updated_by": "testuser",
        "parent_fullname": None,
        "parent_title": None,
        "rating": 100,
        "revisions": 5,
        "children": 0,
        "comments": 3,
        "commented_at": "2025-03-29T10:00:00",
        "commented_by": "commenter",
        "content": "Test content",
        "html": "<p>Test content</p>"
    }
    
    # Call function
    insert_page(mock_db_connection, site, page_data)
    mock_db_connection.commit()
    
    # Verify insertion
    cursor = mock_db_connection.cursor()
    cursor.execute(
        "SELECT fullname, title, rating, revisions FROM pages WHERE site = ? AND fullname = ?",
        (site, page_data["fullname"])
    )
    result = cursor.fetchone()
    
    # Check results
    assert result is not None
    fullname, title, rating, revisions = result
    assert fullname == "scp-001"
    assert title == "SCP-001 Test"
    assert rating == 100
    assert revisions == 5


def test_insert_tags(mock_db_connection):
    """Test inserting tags for a page"""
    # Set up database
    create_tables(mock_db_connection)
    
    # Insert a page first (tags have foreign key constraint)
    site = "scp-wiki"
    page_data = {
        "fullname": "scp-001",
        "title": "SCP-001 Test"
    }
    insert_page(mock_db_connection, site, page_data)
    
    # Tags to insert
    tags = ["scp", "euclid", "test"]
    
    # Call function
    insert_tags(mock_db_connection, site, "scp-001", tags)
    mock_db_connection.commit()
    
    # Verify tags were inserted
    cursor = mock_db_connection.cursor()
    cursor.execute(
        "SELECT tag FROM page_tags WHERE site = ? AND fullname = ? ORDER BY tag",
        (site, "scp-001")
    )
    results = cursor.fetchall()
    
    # Check results
    assert len(results) == 3
    assert [row[0] for row in results] == ["euclid", "scp", "test"]
    
    # Test updating tags
    new_tags = ["keter", "scp", "updated"]
    insert_tags(mock_db_connection, site, "scp-001", new_tags)
    mock_db_connection.commit()
    
    # Verify tags were updated
    cursor.execute(
        "SELECT tag FROM page_tags WHERE site = ? AND fullname = ? ORDER BY tag",
        (site, "scp-001")
    )
    results = cursor.fetchall()
    
    # Check results
    assert len(results) == 3
    assert [row[0] for row in results] == ["keter", "scp", "updated"]


def test_get_db_page_info(mock_db_connection):
    """Test fetching page metadata from database"""
    # Set up database
    create_tables(mock_db_connection)
    
    # Insert test data
    site = "scp-wiki"
    page_data = {
        "fullname": "scp-001",
        "updated_at": "2025-03-30T12:00:00",
        "revisions": 5
    }
    insert_page(mock_db_connection, site, page_data)
    mock_db_connection.commit()
    
    # Call function
    result = get_db_page_info(mock_db_connection, site, "scp-001")
    
    # Verify results
    assert result is not None
    updated_at, revisions = result
    assert updated_at == "2025-03-30T12:00:00"
    assert revisions == 5
    
    # Test for non-existent page
    result = get_db_page_info(mock_db_connection, site, "non-existent")
    assert result is None