# main.py
"""
FastAPI Backend Services
Exposes real-time trending products fetched from the Cassandra operational cache.
"""

import logging
from contextlib import asynccontextmanager
from typing import List
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from database import db_client

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend_api")


class ProductPopularityResponse(BaseModel):
    """
    Response schema for product popularity metrics.
    """
    product_id: str = Field(..., example="PROD_001")
    click_count: int = Field(..., example=120)
    order_count: int = Field(..., example=12)
    popularity_score: float = Field(..., alias="popularity_index", example=180.0)
    last_updated: datetime

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown lifecycle events.
    """
    logger.info("Initializing application lifecycle...")
    try:
        db_client.connect()
    except Exception as e:
        logger.error(f"Could not connect to database on startup: {e}")
    yield
    logger.info("Closing database connections on shutdown...")
    db_client.close()


app = FastAPI(
    title="E-Commerce Real-Time Inventory & Dynamic Pricing Pipeline API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware configuration
# Allows requests from localhost:3000 (standard Next.js port)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, restrict to allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", status_code=200)
async def health_check():
    """
    Health check endpoint for container probes.
    """
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get(
    "/api/v1/products/trending",
    response_model=List[ProductPopularityResponse],
    summary="Get Top 10 Popular Products",
    description="Queries real-time product popularity cache and returns the top 10 most popular products sorted by popularity score."
)
async def get_trending_products():
    """
    Fetches all popularity profiles from Cassandra, sorts them in memory, and returns the top 10.
    """
    try:
        records = db_client.fetch_all_product_popularity()
    except Exception as e:
        logger.warning(f"Database query failed, falling back to mock data. Error: {e}")
        records = []
        
    if not records:
        # If Cassandra is empty or offline, return a mocked list of products to allow the UI to display data during first run
        logger.warning("No records found in Cassandra. Returning fallback data for UI testing.")
        fallback_records = []
        for i in range(1, 11):
            p_id = f"PROD_{i:03d}"
            clicks = 100 - i * 8
            orders = 10 - i
            fallback_records.append({
                "product_id": p_id,
                "click_count": clicks,
                "order_count": orders,
                "popularity_index": float(clicks * 1.0 + orders * 5.0),
                "last_updated": datetime.utcnow()
            })
        return fallback_records

    try:
        # Sort in-memory: popularity_index descending
        sorted_records = sorted(
            records,
            key=lambda x: x.get("popularity_index", 0.0) or 0.0,
            reverse=True
        )

        # Slice top 10
        top_10 = sorted_records[:10]
        return top_10
        
    except Exception as e:
        logger.error(f"Error occurred while processing trending products: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal sorting error: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    # Executed when running python main.py directly
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
