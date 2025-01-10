import logging
from typing import Optional
import psycopg2

logger = logging.getLogger(__name__)

def migrate_up(conn: Optional[psycopg2.extensions.connection] = None) -> None:
    """Create the poi_notifications table."""
    
    up_sql = """
    CREATE TABLE IF NOT EXISTS poi_notifications (
        id SERIAL PRIMARY KEY,
        deployment_id TEXT NOT NULL,
        block_number BIGINT NOT NULL,
        message TEXT NOT NULL,
        sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
        UNIQUE(deployment_id, block_number)
    );

    CREATE INDEX IF NOT EXISTS idx_poi_notifications_sent_at 
    ON poi_notifications(sent_at);
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(up_sql)
        conn.commit()
        logger.info("Successfully created poi_notifications table")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create poi_notifications table: {str(e)}")
        raise

def migrate_down(conn: Optional[psycopg2.extensions.connection] = None) -> None:
    """Remove the poi_notifications table."""
    
    down_sql = """
    DROP TABLE IF EXISTS poi_notifications;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(down_sql)
        conn.commit()
        logger.info("Successfully dropped poi_notifications table")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to drop poi_notifications table: {str(e)}")
        raise 