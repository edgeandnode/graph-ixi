# Import external libraries
import logging
from typing import Dict, Set, List, Optional
import os
import requests

# Import internal modules
from .database import Database
from .notification import SlackNotifier

# Configure logging
logger = logging.getLogger(__name__)


class PoiAnalyzer:
    """Class to analyze POI submissions and notify slack if discrepancies are found."""

    def __init__(self, database: Database, notifier: SlackNotifier):
        """Initialize the POI analyzer with a database and notifier.
        These are internal modules that are injected into the class."""
        self.db = database
        self.notifier = notifier
        self.page_size = 100  # Default page size for pagination

    def analyze_pois(self, deployment_id: str, block_number: int) -> Optional[Dict]:
        """Analyze POI submissions and detect discrepancies between indexers.

        This method checks if different indexers have submitted different POI (Proof of Indexing)
        hashes for the same deployment and block. A discrepancy indicates that indexers
        disagree about the correct POI value.

        Params:
            deployment_id: The deployment CID to analyze
            block_number: The blockchain block number to analyze

        Returns:
            Optional[Dict]: Returns None if:
                - Notification was already sent
                - No POI submissions exist
                - Only one unique POI hash exists (no discrepancy)

                Returns a dictionary containing discrepancy details if multiple different
                POI hashes are found, with structure:
                {
                    'deployment_cid': str,
                    'block_number': int,
                    'submissions': Dict[str, Set[str]],  # POI hash -> set of indexer addresses
                    'reuse_info': Dict[str, List[str]]   # POI reuse history
                }

        Raises:
            Exception: If there's an error accessing the database or processing submissions
        """
        try:
            # Skip if we've already notified about this deployment/block
            if self.db.check_notification_sent(deployment_id, block_number):
                logger.debug(
                    f"Already notified about {deployment_id} at block {block_number}"
                )
                return None

            # Get all POI submissions for this deployment/block
            poi_submissions = self.db.get_latest_pois(deployment_id, block_number)

            if not poi_submissions:
                logger.debug(
                    f"No POI submissions found for {deployment_id} at block {block_number}"
                )
                return None

            # If there's only one POI hash, there's no discrepancy
            if len(poi_submissions) == 1:
                logger.debug(
                    f"All indexers agree on POI for {deployment_id} at block {block_number}"
                )
                return None

            # We have a discrepancy - format the data
            discrepancy_data = {
                "deployment_cid": deployment_id,
                "block_number": block_number,
                "submissions": poi_submissions,
                "reuse_info": self._check_poi_reuse(poi_submissions),
            }

            return discrepancy_data

        except Exception as e:
            logger.error(f"Error analyzing POIs: {str(e)}", exc_info=True)
            raise

    def _check_poi_reuse(
        self, submissions: Dict[str, Set[str]]
    ) -> Dict[str, List[str]]:
        """Check if any POIs have been reused from other blocks/deployments.

        Params:
            submissions: Dictionary mapping POI hashes to sets of indexer addresses

        Returns:
            Dict mapping POI hashes to lists of reuse information
        """
        reuse_info = {}

        query = """
        SELECT 
            p.poi,
            d.ipfs_cid as deployment_id,
            b.number as block_number,
            i.address as indexer_address,
            n.name as network_name,
            p.created_at as submission_time
        FROM pois p
        JOIN sg_deployments d ON d.id = p.sg_deployment_id
        JOIN blocks b ON b.id = p.block_id
        JOIN indexers i ON i.id = p.indexer_id
        JOIN networks n ON n.id = d.network
        WHERE p.poi = ANY(%s)
        ORDER BY p.created_at DESC
        """

        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    poi_hashes = list(submissions.keys())
                    cur.execute(query, (poi_hashes,))
                    results = cur.fetchall()

                    # Group results by POI hash
                    poi_occurrences = {}
                    for row in results:
                        (
                            poi_hash,
                            deployment_id,
                            block_number,
                            indexer_addr,
                            network,
                            timestamp,
                        ) = row
                        if poi_hash not in poi_occurrences:
                            poi_occurrences[poi_hash] = []
                        poi_occurrences[poi_hash].append(
                            {
                                "deployment_id": deployment_id,
                                "block_number": block_number,
                                "indexer_address": indexer_addr,
                                "network": network,
                                "timestamp": timestamp,
                            }
                        )

                    # Format detailed reuse information
                    for poi_hash, occurrences in poi_occurrences.items():
                        if len(occurrences) > 1:  # POI appears more than once
                            reuse_info[poi_hash] = []

                            # Sort by timestamp descending
                            occurrences.sort(key=lambda x: x["timestamp"], reverse=True)

                            # First occurrence is current
                            current = occurrences[0]

                            # Add details for each previous use
                            for prev in occurrences[1:]:
                                time_diff = current["timestamp"] - prev["timestamp"]
                                days_ago = time_diff.days

                                reuse_info[poi_hash].append(
                                    f"Previously used {days_ago} days ago:\n"
                                    f"• Network: {prev['network']}\n"
                                    f"• Deployment: {prev['deployment_id']}\n"
                                    f"• Block: {prev['block_number']}\n"
                                    f"• Indexer: {prev['indexer_address']}"
                                )

                    return reuse_info

        except Exception as e:
            logger.error(f"Error checking POI reuse: {str(e)}", exc_info=True)
            return {}

    def process_new_submissions(self) -> None:
        """Process any new POI submissions and send notifications for discrepancies."""
        try:
            recent_submissions = self._get_recent_submissions()

            for deployment_id, block_number in recent_submissions:
                try:
                    discrepancy = self.analyze_pois(deployment_id, block_number)

                    if discrepancy:
                        # Format and send notification
                        message = self.notifier.format_poi_discrepancy_message(
                            discrepancy
                        )
                        if self.notifier.send_notification(message):
                            # Record that we sent the notification
                            self.db.record_notification(
                                deployment_id, block_number, message
                            )
                except Exception as e:
                    # Log the error but continue processing other submissions
                    logger.error(
                        f"Error processing submission for deployment {deployment_id} at block {block_number}: {str(e)}",
                        exc_info=True,
                    )
                    continue

            # Cleanup old POI notifications from the database
            self.db.cleanup_old_notifications(days=60)

        except Exception as e:
            logger.error(f"Error processing submissions: {str(e)}", exc_info=True)
            raise

    def _get_recent_submissions(self) -> List[tuple[str, int]]:
        """Get list of recent deployment/block combinations to check."""
        graphql_url = os.getenv("GRAPHIX_API_URL", "http://localhost:8000/graphql")
        submissions = set()

        query = """
        query {
            poiAgreementRatios(
                indexerAddress: "%s"
            ) {
                poi {
                    hash
                    block {
                        number
                    }
                    deployment {
                        cid
                    }
                    indexer {
                        address
                    }
                }
            }
        }
        """

        try:
            indexers = self._get_indexers()
            if not indexers:
                logger.error("No indexers found")
                return []

            for indexer_address in indexers:
                logger.debug(f"Fetching POIs for indexer {indexer_address}")
                current_query = query % indexer_address

                response = requests.post(
                    graphql_url, json={"query": current_query}, timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    logger.error(f"GraphQL errors: {data['errors']}")
                    break

                if "data" not in data or "poiAgreementRatios" not in data["data"]:
                    logger.error("Unexpected GraphQL response format")
                    break

                # Extract POIs from current page
                agreements = data["data"]["poiAgreementRatios"]
                for agreement in agreements:
                    submissions.add(
                        (
                            agreement["poi"]["deployment"]["cid"],
                            agreement["poi"]["block"]["number"],
                        )
                    )

            return list(submissions)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch recent submissions: {str(e)}")
            return []

    def _get_indexers(self) -> List[str]:
        """Get list of indexer addresses."""
        query = """
        query {
            indexers(
                limit: %d
            ) {
                address
            }
        }
        """
        try:
            all_indexers = []
            current_query = query % self.page_size
            logger.debug(f"Fetching indexers with limit {self.page_size}")
            response = requests.post(
                os.getenv("GRAPHIX_API_URL"), json={"query": current_query}, timeout=10
            )
            data = response.json()
            if "data" in data and "indexers" in data["data"]:
                indexers = data["data"]["indexers"]
                all_indexers.extend([indexer["address"] for indexer in indexers])
                return all_indexers
            return []
        except Exception as e:
            logger.error(f"Error getting indexers: {str(e)}")
            return []
