"""
Tests for the main processing logic.
"""
import os
import pytest
from unittest.mock import patch, MagicMock

# Import functions from main script
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from store_multi_sites import process_single_page, main


def test_process_single_page(mock_db_connection, mock_server_proxy):
    """Test processing of a single page"""
    # Setup
    site = "scp-wiki"
    page_name = "scp-001"
    meta = {
        "updated_at": "2025-03-30T12:00:00",
        "revisions": 5,
        "tags": ["scp", "euclid", "test"]
    }
    
    # Create tables and prepare DB
    from store_multi_sites import create_tables
    create_tables(mock_db_connection)
    
    # Test case 1: Page doesn't exist in DB yet (should process)
    result = process_single_page(mock_db_connection, mock_server_proxy, site, page_name, meta)
    assert result is True
    
    # Verify page was inserted
    cursor = mock_db_connection.cursor()
    cursor.execute(
        "SELECT fullname FROM pages WHERE site = ? AND fullname = ?",
        (site, page_name)
    )
    assert cursor.fetchone() is not None
    
    # Verify tags were inserted
    cursor.execute(
        "SELECT COUNT(*) FROM page_tags WHERE site = ? AND fullname = ?",
        (site, page_name)
    )
    assert cursor.fetchone()[0] == 3
    
    # Test case 2: Page exists with same metadata (should skip)
    # Reset mocks to count new calls
    mock_server_proxy.pages.get_one.reset_mock()
    
    # Call again with same metadata
    result = process_single_page(mock_db_connection, mock_server_proxy, site, page_name, meta)
    assert result is False  # Should skip
    mock_server_proxy.pages.get_one.assert_not_called()  # Should not call API
    
    # Test case 3: Page exists but needs update (changed revision)
    # Reset mocks
    mock_server_proxy.pages.get_one.reset_mock()
    
    # Call with updated metadata
    updated_meta = {
        "updated_at": "2025-03-31T12:00:00",  # New update time
        "revisions": 6,  # Increased revisions
        "tags": ["scp", "euclid", "test", "new-tag"]
    }
    
    result = process_single_page(mock_db_connection, mock_server_proxy, site, page_name, updated_meta)
    assert result is True  # Should process update
    mock_server_proxy.pages.get_one.assert_called_once()  # Should call API


def test_main_function_with_mocks():
    """Test the main function with all dependencies mocked"""
    with patch('sqlite3.connect') as mock_connect, \
         patch('store_multi_sites.get_server_proxy') as mock_get_proxy, \
         patch('store_multi_sites.select_all_pages') as mock_select_pages, \
         patch('store_multi_sites.get_pages_meta') as mock_get_meta, \
         patch('store_multi_sites.process_single_page') as mock_process_page:
        
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        mock_server = MagicMock()
        mock_get_proxy.return_value = mock_server
        
        # Configure select_all_pages to return a small list for "scp-wiki"
        mock_select_pages.return_value = ["scp-001", "scp-002", "scp-003"]
        
        # Configure get_pages_meta to return test data
        mock_get_meta.return_value = {
            "scp-001": {"updated_at": "2025-03-30", "revisions": 5},
            "scp-002": {"updated_at": "2025-03-29", "revisions": 3},
            "scp-003": {"updated_at": "2025-03-28", "revisions": 1},
        }
        
        # Configure process_single_page to return success
        mock_process_page.return_value = True
        
        # Set environment variables
        with patch.dict(os.environ, {
            "WIKIDOT_API_USER": "test-user",
            "WIKIDOT_API_KEY": "test-key",
            "DB_FILE": ":memory:"
        }):
            # Limit SITES to just "scp-wiki" for the test
            with patch('store_multi_sites.SITES', ["scp-wiki"]):
                # Call main function
                from store_multi_sites import main
                main()
                
                # Verify the core workflow was executed
                mock_connect.assert_called_once()
                mock_get_proxy.assert_called_once()
                mock_select_pages.assert_called_once()
                assert mock_get_meta.call_count > 0
                assert mock_process_page.call_count == 3  # One call per page


if __name__ == "__main__":
    pytest.main(["-v"])