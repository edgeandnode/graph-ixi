import os
import logging
import requests
from typing import Dict, Any

logger = logging.getLogger(__name__)


class SlackNotifier:
    """This class is used to send notifications to a Slack channel."""

    def __init__(self, webhook_url: str = None):
        """
        Initializes the SlackNotifier with a webhook URL.

        Params:
            webhook_url: The Slack webhook URL

        Raises:
            ValueError: If the Slack webhook URL is not provided
        """
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        if not self.webhook_url:
            raise ValueError("Slack webhook URL not provided")

    def send_notification(self, message: str) -> bool:
        """Send a notification to Slack.

        Params:
            message: The formatted message to send

        Returns:
            bool: True if the message was sent successfully
        """
        try:
            response = requests.post(
                self.webhook_url, json={"text": message}, timeout=10
            )
            response.raise_for_status()
            logger.info("Successfully sent Slack notification")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            return False

    def format_poi_discrepancy_message(self, data: Dict[str, Any]) -> str:
        """Format POI discrepancy data into a Slack message.

        Params:
            data: Dictionary containing POI discrepancy information

        Returns:
            str: Formatted message ready to send to Slack
        """
        message_parts = [
            "🚨 *New POI Discrepancy Found*",
            f"*Deployment:* `{data['deployment_cid']}`",
            f"*Block:* `{data['block_number']}`",
            "*POI Submissions:*",
        ]

        for poi_hash, indexers in data["submissions"].items():
            # Convert memoryview to hex string if needed
            if isinstance(poi_hash, memoryview):
                poi_hash = poi_hash.hex()

            submission_parts = [f"*POI Hash:* `{poi_hash}`"]

            # Add POI reuse information if available
            if "reuse_info" in data and poi_hash in data["reuse_info"]:
                reuse_data = data["reuse_info"][poi_hash]
                submission_parts.append("⚠️ *POI Reuse:*")
                for detail in reuse_data:
                    submission_parts.append(f"  • {detail}")

            # Convert indexer addresses if they're memoryview
            formatted_indexers = []
            for indexer in indexers:
                if isinstance(indexer, memoryview):
                    formatted_indexers.append(indexer.hex())
                else:
                    formatted_indexers.append(str(indexer))

            submission_parts.append(
                f"*Submitted by:* `{', '.join(sorted(formatted_indexers))}`"
            )
            message_parts.extend(submission_parts)
            message_parts.append("")  # Add spacing between submissions

        return "\n".join(message_parts)
