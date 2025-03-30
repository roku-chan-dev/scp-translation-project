"""
Tests for Wikidot API interaction functions.
"""
import xmlrpc.client
import pytest
from unittest.mock import patch

# Import functions from main script
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from store_multi_sites import get_server_proxy, select_all_pages, get_pages_meta, get_one_page


def test_get_server_proxy():
    """Test the get_server_proxy function"""
    with patch('xmlrpc.client.ServerProxy') as mock_proxy:
        mock_instance = mock_proxy.return_value
        result = get_server_proxy("test-user", "test-key")
        
        # Verify ServerProxy was called with correct URL format
        mock_proxy.assert_called_once()
        call_args = mock_proxy.call_args[0][0]
        assert "test-user:test-key@wikidot.com/xml-rpc-api.php" in call_args
        assert result == mock_instance


def test_select_all_pages(mock_server_proxy):
    """Test the select_all_pages function"""
    # Configure mock
    mock_server_proxy.pages.select.return_value = ["scp-001", "scp-002", "scp-003"]
    
    # Call function
    result = select_all_pages("scp-wiki", mock_server_proxy)
    
    # Verify results
    mock_server_proxy.pages.select.assert_called_once_with({"site": "scp-wiki"})
    assert result == ["scp-001", "scp-002", "scp-003"]


def test_get_pages_meta(mock_server_proxy):
    """Test the get_pages_meta function"""
    # Configure mock
    expected_meta = {
        "scp-001": {"title": "Test 1", "updated_at": "2025-03-30"},
        "scp-002": {"title": "Test 2", "updated_at": "2025-03-29"}
    }
    mock_server_proxy.pages.get_meta.return_value = expected_meta
    
    # Call function
    result = get_pages_meta("scp-wiki", mock_server_proxy, ["scp-001", "scp-002"])
    
    # Verify results
    mock_server_proxy.pages.get_meta.assert_called_once_with({
        "site": "scp-wiki", 
        "pages": ["scp-001", "scp-002"]
    })
    assert result == expected_meta


def test_get_one_page(mock_server_proxy):
    """Test the get_one_page function"""
    # Configure mock
    expected_page = {
        "fullname": "scp-001",
        "title": "SCP-001 Test",
        "content": "Test content"
    }
    mock_server_proxy.pages.get_one.return_value = expected_page
    
    # Call function
    result = get_one_page("scp-wiki", mock_server_proxy, "scp-001")
    
    # Verify results
    mock_server_proxy.pages.get_one.assert_called_once_with({
        "site": "scp-wiki", 
        "page": "scp-001"
    })
    assert result == expected_page


def test_api_errors(mock_server_proxy):
    """Test error handling in API functions"""
    # Configure mock to raise exception
    mock_server_proxy.pages.get_one.side_effect = xmlrpc.client.Fault(406, "Page does not exist")
    
    # Test with retry disabled (using monkeypatch to override retry decorator)
    with patch('tenacity.retry', lambda **kwargs: lambda f: f):
        with pytest.raises(xmlrpc.client.Fault) as excinfo:
            get_one_page("scp-wiki", mock_server_proxy, "non-existent-page")
        
        # Verify exception details
        assert excinfo.value.faultCode == 406