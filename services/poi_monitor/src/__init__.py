"""
POI Monitor Service

A service that monitors Proof of Indexing (POI) submissions across indexers in The Graph network,
detecting discrepancies and POI reuse.

Components:
- analyzer: POI discrepancy detection
- database: PostgreSQL connection management
- notification: Slack integration
- monitor: Main service loop
- migration: Database schema management
"""

from .analyzer import PoiAnalyzer
from .database import Database
from .notification import SlackNotifier
from .monitor import main

__version__ = "0.1.0"
__all__ = ["PoiAnalyzer", "Database", "SlackNotifier", "main"]
