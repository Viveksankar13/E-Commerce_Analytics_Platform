#!/usr/bin/env python3
"""
E-Commerce Daily Transaction Backup Script
Extracts data from Cassandra (operational profile caches) and Kafka topics (raw queues)
and uploads them as structured backup logs into the ADLS Gen2 Bronze landing area.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from confluent_kafka import Consumer, KafkaError, TopicPartition

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("daily_backup")

# Environment/Configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BACKUP_DIR = os.getenv("BACKUP_DIR", "./backups")

# Azure ADLS Gen2 Configurations
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = "bronze"


def dump_cassandra_table() -> List[Dict[str, Any]]:
    """
    Connects to Cassandra and extracts all records from product_popularity table.
    """
    logger.info(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect("ecommerce_analytics")
    
    query = "SELECT product_id, click_count, order_count, popularity_index, last_updated FROM product_popularity;"
    logger.info("Executing Cassandra query to fetch popularity profiles...")
    
    rows = session.execute(query)
    results = []
    for r in rows:
        results.append({
            "product_id": r.product_id,
            "click_count": r.click_count,
            "order_count": r.order_count,
            "popularity_index": r.popularity_index,
            "last_updated": r.last_updated.isoformat() if r.last_updated else None
        })
        
    cluster.shutdown()
    logger.info(f"Extracted {len(results)} rows from Cassandra.")
    return results


def consume_kafka_topic(topic: str, max_messages: int = 5000) -> List[Dict[str, Any]]:
    """
    Consumes outstanding/buffered messages from a specific Kafka topic.
    Uses a temporary consumer group to scrape currently available messages without blocking.
    """
    logger.info(f"Consuming messages from Kafka topic '{topic}'...")
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'backup-consumer-{topic}-{int(datetime.now().timestamp())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    records = []
    empty_polls = 0
    max_empty_polls = 5  # Stop after 5 consecutive empty polls (no more messages)
    
    # Read messages
    while len(records) < max_messages and empty_polls < max_empty_polls:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not a hard error
                continue
            else:
                logger.error(f"Kafka error encountered: {msg.error()}")
                break
                
        empty_polls = 0  # Reset empty poll counter
        
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            records.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode('utf-8') if msg.key() else None,
                "payload": payload,
                "timestamp": msg.timestamp()[1] if msg.timestamp() else None
            })
        except Exception as e:
            logger.error(f"Error parsing message value: {e}")
            
    consumer.close()
    logger.info(f"Extracted {len(records)} messages from Kafka topic '{topic}'.")
    return records


def upload_to_adls(local_filepath: str, remote_blob_name: str) -> bool:
    """
    Uploads a local backup file to Azure ADLS Gen2 if configured.
    """
    if not AZURE_STORAGE_CONNECTION_STRING:
        logger.warning("AZURE_STORAGE_CONNECTION_STRING is not set. Skipping Azure upload.")
        return False
        
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        logger.info(f"Uploading {local_filepath} to ADLS Gen2 as {remote_blob_name}...")
        
        service_client = DataLakeServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER_NAME)
        
        # Ensure directories exist
        directory_name = os.path.dirname(remote_blob_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        if not directory_client.exists():
            directory_client.create_directory()
            
        file_client = file_system_client.get_file_client(remote_blob_name)
        with open(local_filepath, "rb") as data:
            file_client.upload_data(data, overwrite=True)
            
        logger.info("ADLS Gen2 upload successful.")
        return True
    except ImportError:
        logger.warning("azure-storage-file-datalake library not installed. Cannot upload to Azure.")
        return False
    except Exception as e:
        logger.error(f"Failed to upload to Azure ADLS Gen2: {e}")
        return False


def main() -> None:
    # 1. Create local backup directory
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    date_path_str = datetime.now(timezone.utc).strftime("year=%Y/month=%m/day=%d")
    
    # 2. Extract and backup Cassandra records
    try:
        cassandra_data = dump_cassandra_table()
        cassandra_filename = f"cassandra_popularity_backup_{timestamp_str}.json"
        cassandra_filepath = os.path.join(BACKUP_DIR, cassandra_filename)
        
        with open(cassandra_filepath, "w") as f:
            json.dump(cassandra_data, f, indent=2)
        logger.info(f"Saved Cassandra data locally to {cassandra_filepath}")
        
        # Upload to ADLS Gen2 Bronze
        remote_c_path = f"cassandra_backups/{date_path_str}/{cassandra_filename}"
        upload_to_adls(cassandra_filepath, remote_c_path)
    except Exception as e:
        logger.error(f"Failed Cassandra backup process: {e}")

    # 3. Extract and backup Kafka Topics (Clicks and Orders)
    for topic in ["clicks", "orders"]:
        try:
            kafka_data = consume_kafka_topic(topic)
            kafka_filename = f"kafka_{topic}_backup_{timestamp_str}.json"
            kafka_filepath = os.path.join(BACKUP_DIR, kafka_filename)
            
            with open(kafka_filepath, "w") as f:
                json.dump(kafka_data, f, indent=2)
            logger.info(f"Saved Kafka topic {topic} data locally to {kafka_filepath}")
            
            # Upload to ADLS Gen2 Bronze
            remote_k_path = f"clicks/{date_path_str}/{kafka_filename}" if topic == "clicks" else f"orders/{date_path_str}/{kafka_filename}"
            upload_to_adls(kafka_filepath, remote_k_path)
        except Exception as e:
            logger.error(f"Failed Kafka backup process for topic '{topic}': {e}")
            
    logger.info("Daily backup run complete.")


if __name__ == "__main__":
    main()
