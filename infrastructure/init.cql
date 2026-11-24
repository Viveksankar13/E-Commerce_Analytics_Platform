-- init.cql
-- Create keyspace if not exists
CREATE KEYSPACE IF NOT EXISTS ecommerce_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Switch to the keyspace
USE ecommerce_analytics;

-- Create product_popularity table to cache real-time state profiles
CREATE TABLE IF NOT EXISTS product_popularity (
    product_id text PRIMARY KEY,
    click_count int,
    order_count int,
    popularity_index double,
    last_updated timestamp
);
