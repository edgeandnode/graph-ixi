import os
import logging
import importlib
from typing import List
import psycopg2

logger = logging.getLogger(__name__)

class MigrationManager:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self._ensure_migration_table()

    def _ensure_migration_table(self):
        """Create the migrations tracking table if it doesn't exist."""
        sql = """
        CREATE TABLE IF NOT EXISTS poi_monitor_migrations (
            id SERIAL PRIMARY KEY,
            migration_name TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    def get_applied_migrations(self) -> List[str]:
        """Get list of already applied migrations."""
        sql = "SELECT migration_name FROM poi_monitor_migrations ORDER BY id;"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]

    def apply_migrations(self):
        """Apply all pending migrations."""
        applied = set(self.get_applied_migrations())
        
        # Get all migration files
        migrations_dir = os.path.join(os.path.dirname(__file__), '..', 'migrations')
        logger.info(f"Looking for migrations in: {migrations_dir}")
        migration_files = sorted([
            f for f in os.listdir(migrations_dir)
            if (f.endswith('.py') and f != '__init__.py') or f.endswith('.sql')
        ])
        logger.info(f"Found migration files: {migration_files}")

        for migration_file in migration_files:
            migration_name = migration_file[:-3]
            
            if migration_name in applied:
                logger.info(f"Skipping already applied migration: {migration_name}")
                continue

            logger.info(f"Applying migration: {migration_name}")
            
            try:
                if migration_file.endswith('.sql'):
                    # Handle SQL files
                    with open(os.path.join(migrations_dir, migration_file)) as f:
                        sql = f.read()
                    with self.conn.cursor() as cur:
                        cur.execute(sql)
                    self.conn.commit()
                else:
                    # Import and run Python migrations
                    module = importlib.import_module(f"migrations.{migration_name}")
                    module.migrate_up(self.conn)
                
                # Record the migration
                with self.conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO poi_monitor_migrations (migration_name) VALUES (%s)",
                        (migration_name,)
                    )
                self.conn.commit()
                
                logger.info(f"Successfully applied migration: {migration_name}")
                
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Failed to apply migration {migration_name}: {str(e)}")
                raise 