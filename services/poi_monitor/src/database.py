import os
import logging
from typing import Dict, Set
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class Database:
    """
    This class is the Database manager for POI monitoring.

    This class manages database connections and provides methods for:
    - Tracking POI submissions across indexers
    - Managing slack notification history
    - Handling database migrations (schema updates)
    """

    def __init__(self):
        """
        Constructor initializes database connection and runs migrations.

        Raises:
            psycopg2.Error: If database connection cannot be established
            Exception: If migrations fail to apply
        """
        # Create the connection pool first
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=os.getenv("POSTGRES_DB", "graphix"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5433"),
        )

        # Get initial connection for migrations
        with self.get_connection() as conn:
            self.conn = conn  # Store temporary reference for migrations
            self._run_migrations()
            self.conn = None  # Remove reference after migrations

    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool with automatic cleanup (this is faster than creating a
        new connetion each time we want to talk to the db)

        - Automatically returns connection to pool after use
        - Handles cleanup even if an exception occurs
        """
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    def get_latest_pois(
        self, deployment_id: str, block_number: int
    ) -> Dict[str, Set[str]]:
        """Fetch all indexer POI submissions for a specific deployment and block.

        Retrieves all POI submissions and the indexers that submitted them for
        a given deployment at a specific block number.

        Params:
            deployment_id: The IPFS CID of the subgraph deployment
            block_number: The block number to verify POIs against

        Returns:
            A dictionary mapping POI hashes to sets of indexer addresses.
            Example:
            {
                "0xabc...": {"0x123...", "0x456..."},
                "0xdef...": {"0x789..."}
            }
        """
        query = """
        SELECT p.poi, i.address 
        FROM pois p
        JOIN indexers i ON i.id = p.indexer_id
        JOIN blocks b ON b.id = p.block_id
        JOIN sg_deployments d ON d.id = p.sg_deployment_id
        WHERE d.ipfs_cid = %s AND b.number = %s
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (deployment_id, block_number))
                results = cur.fetchall()

                poi_submissions = {}
                for poi_hash, indexer_addr in results:
                    if poi_hash not in poi_submissions:
                        poi_submissions[poi_hash] = set()
                    poi_submissions[poi_hash].add(indexer_addr)

                return poi_submissions

    def check_notification_sent(self, deployment_id: str, block_number: int) -> bool:
        """Check if we've already notified about the current set of POIs for this deployment/block.

        Params:
            deployment_id: The deployment CID
            block_number: The block number

        Returns:
            bool: True if we've already notified about these exact POIs
        """
        query = """
        WITH current_pois AS (
            SELECT array_agg(DISTINCT p.poi ORDER BY p.poi) as poi_set
            FROM pois p
            JOIN blocks b ON b.id = p.block_id
            JOIN sg_deployments d ON d.id = p.sg_deployment_id
            WHERE d.ipfs_cid = %s AND b.number = %s
        )
        SELECT EXISTS (
            SELECT 1 
            FROM poi_notifications n
            CROSS JOIN current_pois c
            WHERE n.deployment_id = %s 
            AND n.block_number = %s
            AND n.poi_set = c.poi_set
        )
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query, (deployment_id, block_number, deployment_id, block_number)
                )
                return cur.fetchone()[0]

    def record_notification(
        self, deployment_id: str, block_number: int, message: str
    ) -> None:
        """Record that a notification was sent. Later used to prevent duplicate notifications/spam.

        Params:
            deployment_id: The deployment IPFS hash
            block_number: The block number
            message: The notification message that was sent
        """

        query = """
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
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (deployment_id, block_number, deployment_id, block_number, message),
                )
            conn.commit()

    def cleanup_old_notifications(self, days: int = 60) -> None:
        """Remove notification records older than specified days."""
        query = """
        DELETE FROM poi_notifications 
        WHERE sent_at < NOW() - INTERVAL '%s days'
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (days,))
            conn.commit()

    def _run_migrations(self):
        """Run any pending database migrations."""
        from .migration import MigrationManager

        try:
            manager = MigrationManager(self.conn)
            manager.apply_migrations()

            # Verify table structure
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'poi_notifications'
                """)
                columns = cur.fetchall()
                logger.info(f"poi_notifications table structure: {columns}")

        except Exception as e:
            logger.error(f"Failed to run migrations: {str(e)}")
            raise
