import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from src.database import Database
import psycopg2

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    with patch('psycopg2.connect', return_value=conn):
        yield conn

@pytest.fixture
def database(mock_conn):
    """Create a Database instance with mocked connection."""
    with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
        # Mock the connection pool
        pool = MagicMock()
        pool.getconn.return_value = mock_conn
        mock_pool.return_value = pool
        
        # Mock migrations
        with patch('src.migration.MigrationManager') as mock_manager:
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            
            db = Database()
            return db

def test_database_connection_retry(mock_conn):
    """Test that database connection retries on failure."""
    with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
        # Make pool creation fail twice then succeed
        mock_pool.side_effect = [
            psycopg2.Error("Test error"),
            psycopg2.Error("Test error"),
            MagicMock()  # Successful pool
        ]
        
        # Mock migrations
        with patch('src.migration.MigrationManager') as mock_manager:
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            with patch('time.sleep'):  # Don't actually sleep in tests
                db = Database()
                # Verify pool was created
                assert db.pool is not None

def test_get_latest_pois(database, mock_conn):
    """Test fetching latest POI submissions."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("poi1", "indexer1"),
        ("poi1", "indexer2"),
        ("poi2", "indexer3")
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    result = database.get_latest_pois("deployment1", 1000)
    
    assert len(result) == 2  # Two unique POIs
    assert result["poi1"] == {"indexer1", "indexer2"}
    assert result["poi2"] == {"indexer3"}

def test_check_notification_sent(database, mock_conn):
    """Test checking if notification was already sent."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (True,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    result = database.check_notification_sent("deployment1", 1000)
    assert result is True

def test_record_notification(database, mock_conn):
    """Test recording a notification."""
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    database.record_notification("deployment1", 1000, "test message")
    
    # Verify the INSERT query was executed with correct parameters
    mock_cursor.execute.assert_any_call(
        """
        WITH current_pois AS (
            SELECT array_agg(DISTINCT p.poi ORDER BY p.poi) as poi_set
            FROM pois p
            JOIN blocks b ON b.id = p.block_id
            JOIN sg_deployments d ON d.id = p.sg_deployment_id
            WHERE d.ipfs_cid = %s AND b.number = %s
        )
        INSERT INTO poi_notifications (deployment_id, block_number, message, sent_at, poi_set)
        SELECT %s, %s, %s, NOW(), c.poi_set::bytea[]
        FROM current_pois c
        """,
        ("deployment1", 1000, "deployment1", 1000, "test message")
    )
    mock_conn.commit.assert_called_once()

def test_cleanup_old_notifications(database, mock_conn):
    """Test cleaning up old notifications."""
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    database.cleanup_old_notifications(days=30)
    
    # Verify the DELETE query was executed with correct parameters
    mock_cursor.execute.assert_any_call(
        """
        DELETE FROM poi_notifications 
        WHERE sent_at < NOW() - INTERVAL '%s days'
        """,
        (30,)
    )
    mock_conn.commit.assert_called_once()