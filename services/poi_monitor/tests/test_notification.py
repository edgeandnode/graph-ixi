import pytest
from unittest.mock import patch
from src.notification import SlackNotifier
import requests


@pytest.fixture
def notifier():
    with patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "http://test.url"}):
        return SlackNotifier()


def test_send_notification_success(notifier):
    """Test successful notification sending."""
    with patch("requests.post") as mock_post:
        mock_post.return_value.raise_for_status.return_value = None

        result = notifier.send_notification("test message")

        assert result is True
        mock_post.assert_called_once()


def test_send_notification_failure(notifier):
    """Test handling of notification failure."""
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.RequestException("Test error")

        result = notifier.send_notification("test message")

        assert result is False


def test_format_poi_discrepancy_message(notifier):
    """Test message formatting."""
    data = {
        "deployment_cid": "Qm123",
        "block_number": 1000,
        "submissions": {"poi1": {"indexer1", "indexer2"}, "poi2": {"indexer3"}},
        "reuse_info": {"poi2": ["Previously used in deployment X"]},
    }

    message = notifier.format_poi_discrepancy_message(data)

    assert "🚨" in message
    assert "Qm123" in message
    assert "indexer1" in message
    assert "Previously used in deployment X" in message
