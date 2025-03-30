"""
Test fixtures for SCP Translation Project tests.
"""
import os
import sqlite3
import tempfile
import pytest
import xmlrpc.client
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables"""
    monkeypatch.setenv("WIKIDOT_API_USER", "test-user")
    monkeypatch.setenv("WIKIDOT_API_KEY", "test-key")
    # Create a temporary db file path
    temp_db = os.path.join(tempfile.gettempdir(), "test_scp_data.sqlite")
    monkeypatch.setenv("DB_FILE", temp_db)
    yield temp_db
    # Cleanup temp db file if it exists
    if os.path.exists(temp_db):
        os.unlink(temp_db)


@pytest.fixture
def mock_db_connection():
    """Fixture to create a test database connection"""
    # Create a temporary in-memory database
    conn = sqlite3.connect(":memory:")
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


@pytest.fixture
def mock_server_proxy():
    """Fixture to mock xmlrpc.client.ServerProxy"""
    with patch("xmlrpc.client.ServerProxy") as mock_proxy:
        server = MagicMock()
        mock_proxy.return_value = server
        
        # Configure common mock responses
        pages = MagicMock()
        server.pages = pages
        
        # pages.select mock
        pages.select.return_value = ["scp-001", "scp-002", "scp-003"]
        
        # pages.get_meta mock
        meta_data = {
            "scp-001": {
                "updated_at": "2025-03-30T12:00:00",
                "revisions": 5,
                "tags": ["scp", "euclid", "test"]
            },
            "scp-002": {
                "updated_at": "2025-03-29T10:00:00",
                "revisions": 3,
                "tags": ["scp", "safe", "test"]
            }
        }
        pages.get_meta.return_value = meta_data
        
        # pages.get_one mock
        page_data = {
            "fullname": "scp-001",
            "title": "SCP-001 Test",
            "content": "Test content",
            "html": "<p>Test content</p>",
            "created_at": "2025-01-01T00:00:00",
            "created_by": "testuser",
            "updated_at": "2025-03-30T12:00:00",
            "updated_by": "testuser",
            "rating": 100,
            "revisions": 5,
            "tags": ["scp", "euclid", "test"]
        }
        pages.get_one.return_value = page_data
        
        yield server