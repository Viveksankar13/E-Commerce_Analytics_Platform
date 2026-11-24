# database.py
"""
Cassandra Database Connection Manager
Handles connection lifecycle to Cassandra cluster.
"""

import os
import logging
from typing import Optional, List, Dict, Any
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

logger = logging.getLogger("backend_db")


class CassandraDB:
    """
    Cassandra Database Manager using Cassandra driver.
    """
    def __init__(self) -> None:
        self.hosts: List[str] = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        self.port: int = int(os.getenv("CASSANDRA_PORT", 9042))
        self.keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "ecommerce_analytics")
        
        # Security/Authentication
        self.username: Optional[str] = os.getenv("CASSANDRA_USERNAME")
        self.password: Optional[str] = os.getenv("CASSANDRA_PASSWORD")
        
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

    def connect(self) -> None:
        """
        Establishes a connection to the Cassandra cluster and sets up default keyspace.
        """
        logger.info(f"Connecting to Cassandra hosts: {self.hosts} on port {self.port}...")
        
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(username=self.username, password=self.password)
            
        try:
            self.cluster = Cluster(
                contact_points=self.hosts,
                port=self.port,
                auth_provider=auth_provider,
                connect_timeout=10.0
            )
            self.session = self.cluster.connect()
            
            # Use dictionary factory to retrieve rows as dictionaries instead of tuples
            self.session.row_factory = dict_factory
            
            # Ensure target keyspace is selected
            self.session.set_keyspace(self.keyspace)
            logger.info(f"Successfully connected to Cassandra keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra cluster: {e}")
            raise e

    def fetch_all_product_popularity(self) -> List[Dict[str, Any]]:
        """
        Queries all records from the product_popularity table.
        Sorting is executed in-memory in the API layer due to Cassandra restrictions.
        """
        if not self.session:
            raise RuntimeError("Database session not initialized. Call connect() first.")
            
        query = "SELECT product_id, click_count, order_count, popularity_index, last_updated FROM product_popularity;"
        try:
            rows = self.session.execute(query)
            return list(rows)
        except Exception as e:
            logger.error(f"Error querying product_popularity: {e}")
            return []

    def close(self) -> None:
        """
        Safely closes the cluster session and connection.
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("Cassandra database connection closed.")


# Single instance database connection manager
db_client = CassandraDB()
