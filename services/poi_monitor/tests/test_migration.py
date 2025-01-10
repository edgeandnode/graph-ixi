import pytest
from unittest.mock import Mock, MagicMock, patch
from src.migration import MigrationManager
import os

@pytest.fixture
def mock_conn():
    return MagicMock()

@pytest.fixture
def manager(mock_conn):
    return MigrationManager(mock_conn)

def test_ensure_migration_table(manager, mock_conn):
    """Test migration table creation."""
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Reset the mock to clear any previous calls
    mock_conn.commit.reset_mock()
    
    manager._ensure_migration_table()
    
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()

def test_get_applied_migrations(manager, mock_conn):
    """Test fetching applied migrations."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("001_create_notifications_table",),
        ("002_another_migration",)
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    result = manager.get_applied_migrations()
    assert len(result) == 2
    assert "001_create_notifications_table" in result 