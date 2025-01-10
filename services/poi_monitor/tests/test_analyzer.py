import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from src.analyzer import PoiAnalyzer
from src.database import Database
from src.notification import SlackNotifier
import requests

@pytest.fixture
def mock_db():
    db = Mock(spec=Database)
    # Create a context manager mock
    context_mock = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Set up the context manager chain
    db.get_connection.return_value = context_mock
    context_mock.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Store cursor on db for tests that need it
    db._test_cursor = mock_cursor
    return db  # Return just the db, not the tuple

@pytest.fixture
def mock_notifier():
    return Mock(spec=SlackNotifier)

@pytest.fixture
def analyzer(mock_db, mock_notifier):
    return PoiAnalyzer(mock_db, mock_notifier)

def test_analyze_pois_no_discrepancy(analyzer, mock_db):
    # Setup
    deployment_id = "Qm123"
    block_number = 1000
    
    # Mock database responses
    mock_db.check_notification_sent.return_value = False
    mock_db.get_latest_pois.return_value = {
        "poi_hash_1": {"indexer1", "indexer2"}  # Single POI hash = no discrepancy
    }
    
    # Execute
    result = analyzer.analyze_pois(deployment_id, block_number)
    
    # Assert
    assert result is None
    mock_db.check_notification_sent.assert_called_once_with(deployment_id, block_number)
    mock_db.get_latest_pois.assert_called_once_with(deployment_id, block_number)

def test_analyze_pois_with_discrepancy(analyzer, mock_db):
    # Setup
    deployment_id = "Qm123"
    block_number = 1000
    
    # Mock database responses
    mock_db.check_notification_sent.return_value = False
    mock_db.get_latest_pois.return_value = {
        "poi_hash_1": {"indexer1"},
        "poi_hash_2": {"indexer2"}  # Two different POI hashes = discrepancy
    }
    
    # Execute
    result = analyzer.analyze_pois(deployment_id, block_number)
    
    # Assert
    assert result is not None
    assert result["deployment_cid"] == deployment_id
    assert result["block_number"] == block_number
    assert result["submissions"] == mock_db.get_latest_pois.return_value

def test_check_poi_reuse(analyzer):
    """Test POI reuse detection."""
    # Setup
    submissions = {
        "poi_hash_1": {"indexer1"},
        "poi_hash_2": {"indexer2"}
    }

    # Mock database responses
    mock_cursor = analyzer.db._test_cursor
    mock_cursor.fetchall.return_value = [
        # Match the columns from the query:
        # poi, deployment_id, block_number, indexer_address, network_name, submission_time
        ("poi_hash_1", "deployment1", 1000, b"addr1", "mainnet", datetime.now()),
        ("poi_hash_1", "deployment2", 900, b"addr2", "mainnet", datetime.now())
    ]

    # Execute
    result = analyzer._check_poi_reuse(submissions)

    # Assert
    assert "poi_hash_1" in result
    assert len(result["poi_hash_1"]) == 1
    assert "Previously used" in result["poi_hash_1"][0]

def test_analyze_pois_already_notified(analyzer, mock_db):
    """Test that we don't re-notify about known discrepancies."""
    deployment_id = "Qm123"
    block_number = 1000
    
    mock_db.check_notification_sent.return_value = True
    
    result = analyzer.analyze_pois(deployment_id, block_number)
    assert result is None
    mock_db.get_latest_pois.assert_not_called()

def test_analyze_pois_no_submissions(analyzer, mock_db):
    """Test handling of blocks with no POI submissions."""
    deployment_id = "Qm123"
    block_number = 1000
    
    mock_db.check_notification_sent.return_value = False
    mock_db.get_latest_pois.return_value = {}
    
    result = analyzer.analyze_pois(deployment_id, block_number)
    assert result is None

def test_process_new_submissions_handles_errors(analyzer, mock_db):
    """Test error handling in the main processing loop."""
    # Mock _get_recent_submissions to return some test data
    analyzer._get_recent_submissions = Mock(return_value=[
        ("Qm123", 1000),
        ("Qm456", 2000)
    ])
    
    # Make analyze_pois raise an exception for the second submission
    def mock_analyze(deployment_id, block_number):
        if deployment_id == "Qm456":
            raise Exception("Test error")
        return None
    
    analyzer.analyze_pois = Mock(side_effect=mock_analyze)
    mock_db.cleanup_old_notifications = Mock()
    
    # This should not raise an exception and should continue processing
    analyzer.process_new_submissions()
    
    # Verify we tried to process both submissions
    assert analyzer.analyze_pois.call_count == 2
    # Verify cleanup was still called
    mock_db.cleanup_old_notifications.assert_called_once()

def test_get_recent_submissions_handles_api_errors(analyzer):
    """Test handling of GraphQL API errors."""
    with patch('requests.post') as mock_post:
        # Mock a failed API response
        mock_post.side_effect = requests.exceptions.RequestException("API Error")
        
        result = analyzer._get_recent_submissions()
        assert result == []  # Should return empty list on error

def test_check_poi_reuse_with_multiple_reuses(analyzer):
    """Test POI reuse detection with multiple reuse patterns."""
    submissions = {
        "poi_hash_1": {"indexer1"},
        "poi_hash_2": {"indexer2"}
    }
    
    now = datetime.now()
    
    # Mock database responses
    mock_cursor = analyzer.db._test_cursor
    mock_cursor.fetchall.return_value = [
        ("poi_hash_1", "deployment1", 1000, b"addr1", "mainnet", now),
        ("poi_hash_1", "deployment2", 900, b"addr2", "mainnet", now),
        ("poi_hash_2", "deployment1", 1000, b"addr1", "mainnet", now),
        ("poi_hash_2", "deployment1", 950, b"addr1", "mainnet", now)
    ]
    
    result = analyzer._check_poi_reuse(submissions)
    assert len(result) == 2  # Both POIs were reused 