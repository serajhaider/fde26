import os
import sqlite3
from fastapi import FastAPI, HTTPException

# Completely disable OpenAPI / Swagger / ReDoc UI documentation
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# Path inside the Docker container
DB_PATH = os.getenv("DB_PATH", "/app/data/product_translation_and_geolocation.db")


def get_db():
    if not os.path.exists(DB_PATH):
        raise HTTPException(
            status_code=500, detail=f"Database file not found at {DB_PATH}"
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Returns rows as key-value dicts
    return conn


@app.get("/translation")
def get_translations():
    """Returns category translations as a pure JSON array."""
    with get_db() as conn:
        cursor = conn.cursor()
        rows = cursor.execute("SELECT * FROM translation").fetchall()
        return [dict(row) for row in rows]


@app.get("/geolocation")
def get_geolocation(page: int = 1, limit: int = 1000):
    """Returns paginated geolocation records as a structured JSON object."""
    if page < 1 or limit < 1:
        raise HTTPException(
            status_code=400, detail="Page and limit must be positive integers."
        )

    offset = (page - 1) * limit

    with get_db() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(
            "SELECT * FROM geolocation LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()

        data = [dict(row) for row in rows]
        has_next = len(data) == limit
        next_url = (
            f"http://localhost:8001/geolocation?page={page + 1}&limit={limit}"
            if has_next
            else None
        )

        return {
            "page": page,
            "limit": limit,
            "count": len(data),
            "next_url": next_url,
            "data": data,
        }