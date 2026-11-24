#!/usr/bin/env python3
"""
E-Commerce Real-Time Event Generator
Simulates clickstream and order transactions and publishes them to Apache Kafka.
"""

import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Dict, Any, List

from confluent_kafka import Producer, KafkaError

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("data_generator")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CLICKS_TOPIC = "clicks"
ORDERS_TOPIC = "orders"
NUM_PRODUCTS = 1000
PROD_PREFIX = "PROD_"

# Global exit flag
shutdown_event = Event()


def generate_product_catalog(num_products: int) -> Dict[str, Dict[str, Any]]:
    """
    Generates a static product catalog with weights and base prices.
    Weights follow a power-law distribution to simulate product popularity.
    """
    catalog = {}
    random.seed(42)  # Consistent catalog generation
    
    # Generate products with power-law weights (Zipf distribution simulation)
    for i in range(1, num_products + 1):
        product_id = f"{PROD_PREFIX}{i:03d}"
        # Price ranges from $5.00 to $1200.00
        base_price = round(random.uniform(5.00, 1200.00), 2)
        # Power-law weight: earlier products are exponentially more popular
        weight = 1.0 / (i ** 0.5)
        
        catalog[product_id] = {
            "product_id": product_id,
            "base_price": base_price,
            "weight": weight
        }
    return catalog


# Initialize catalog
PRODUCT_CATALOG = generate_product_catalog(NUM_PRODUCTS)
PRODUCT_LIST = list(PRODUCT_CATALOG.keys())
PRODUCT_WEIGHTS = [PRODUCT_CATALOG[p]["weight"] for p in PRODUCT_LIST]


def delivery_report(err: Any, msg: Any) -> None:
    """
    Optional per-message delivery callback for Kafka producer.
    Called once for each message sent to indicate success or failure.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # Avoid flooding logs, only log randomly or for specific errors
        if random.random() < 0.001:  # Log 0.1% of successful writes
            logger.info(f"Delivered message to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


class ClickStreamProducer(Thread):
    """
    Simulates user clicks (product views) and publishes to Kafka.
    """
    def __init__(self, producer: Producer, topic: str):
        super().__init__(name="ClickGeneratorThread")
        self.producer = producer
        self.topic = topic

    def run(self) -> None:
        logger.info("Starting ClickStream Simulator...")
        actions = ["view_product"]
        
        while not shutdown_event.is_set():
            try:
                # Select a product based on popularity weights
                product_id = random.choices(PRODUCT_LIST, weights=PRODUCT_WEIGHTS, k=1)[0]
                user_id = f"USR_{random.randint(100000, 999999)}"
                session_id = f"SES_{random.randint(100000000, 999999999)}"
                
                payload = {
                    "user_id": user_id,
                    "action": random.choice(actions),
                    "product_id": product_id,
                    "session_id": session_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                self.producer.produce(
                    self.topic,
                    key=product_id.encode('utf-8'),
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                
                # Poll to serve delivery callbacks
                self.producer.poll(0)
                
                # Sleep a short duration to match high-throughput click rates (100-500ms)
                time.sleep(random.uniform(0.01, 0.05))
                
            except Exception as e:
                logger.error(f"Error in ClickStreamProducer: {e}")
                time.sleep(1)
                
        # Flush remaining messages
        logger.info("Flushing ClickStream remaining messages...")
        self.producer.flush()
        logger.info("ClickStream Simulator stopped.")


class OrderStreamProducer(Thread):
    """
    Simulates user orders (transactions) and publishes to Kafka.
    """
    def __init__(self, producer: Producer, topic: str):
        super().__init__(name="OrderGeneratorThread")
        self.producer = producer
        self.topic = topic

    def run(self) -> None:
        logger.info("Starting OrderStream Simulator...")
        statuses = ["success", "success", "success", "success", "failed"]  # 80% success
        
        while not shutdown_event.is_set():
            try:
                # Select a product based on popularity weights
                product_id = random.choices(PRODUCT_LIST, weights=PRODUCT_WEIGHTS, k=1)[0]
                product_info = PRODUCT_CATALOG[product_id]
                
                user_id = f"USR_{random.randint(100000, 999999)}"
                order_id = f"ORD_{random.randint(10000000, 99999999)}"
                
                # Add minor price variance
                price = round(product_info["base_price"] * random.uniform(0.95, 1.05), 2)
                quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 8, 5, 2], k=1)[0]
                
                payload = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "price": price,
                    "quantity": quantity,
                    "status": random.choice(statuses),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                self.producer.produce(
                    self.topic,
                    key=product_id.encode('utf-8'),
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                
                # Poll to serve delivery callbacks
                self.producer.poll(0)
                
                # Sleep longer since orders are less frequent than clicks (200-1000ms)
                time.sleep(random.uniform(0.05, 0.25))
                
            except Exception as e:
                logger.error(f"Error in OrderStreamProducer: {e}")
                time.sleep(1)
                
        # Flush remaining messages
        logger.info("Flushing OrderStream remaining messages...")
        self.producer.flush()
        logger.info("OrderStream Simulator stopped.")


def sigint_handler(signum: int, frame: Any) -> None:
    """
    Handles SIGINT / KeyboardInterrupt gracefully.
    """
    logger.info("SIGINT received! Shutting down simulators...")
    shutdown_event.set()


def main() -> None:
    # Register signal handler
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    # Kafka Producer Config (Production Tuning)
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'ecommerce-data-generator',
        # Throughput optimizations
        'acks': '1',                       # Acknowledge by leader
        'compression.type': 'snappy',       # Efficient CPU & network compression
        'linger.ms': 10,                   # Wait 10ms for batching
        'batch.size': 16384,               # 16KB batch size
        # Reliability configs
        'retries': 5,
        'retry.backoff.ms': 100
    }

    try:
        producer = Producer(producer_config)
    except KafkaError as ke:
        logger.error(f"Failed to create Kafka producer: {ke}")
        sys.exit(1)

    # Start generators
    click_thread = ClickStreamProducer(producer, CLICKS_TOPIC)
    order_thread = OrderStreamProducer(producer, ORDERS_TOPIC)

    click_thread.start()
    order_thread.start()

    # Block main thread until signal received
    while not shutdown_event.is_set():
        time.sleep(0.5)

    # Wait for threads to finish
    click_thread.join()
    order_thread.join()
    logger.info("Generator program exited cleanly.")


if __name__ == "__main__":
    main()
