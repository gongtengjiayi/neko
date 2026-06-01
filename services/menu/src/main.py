"""
NekoCafé 菜单微服务 (Menu Service)
- 菜品分类与菜品 CRUD
- Kafka 事件发布
"""

import os
import uuid
import json
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("menu-service")

DB_HOST = os.getenv("DB_HOST", "postgres-menu")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "menu")
DB_USER = os.getenv("DB_USER", "menu")
DB_PASS = os.getenv("DB_PASS", "menu123")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

db_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[AIOKafkaProducer] = None


# ============================================================
# Models
# ============================================================

class CategoryCreate(BaseModel):
    name: str = Field(..., max_length=50)
    sort_order: int = Field(0, ge=0)

class CategoryUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=50)
    sort_order: Optional[int] = Field(None, ge=0)

class MenuItemCreate(BaseModel):
    category_id: str
    name: str = Field(..., max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    price: float = Field(..., gt=0)
    image_url: Optional[str] = None
    is_available: bool = True

class MenuItemUpdate(BaseModel):
    category_id: Optional[str] = None
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    price: Optional[float] = Field(None, gt=0)
    image_url: Optional[str] = None
    is_available: Optional[bool] = None


# ============================================================
# Database init
# ============================================================

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=3, max_size=10,
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id          UUID PRIMARY KEY,
                name        VARCHAR(50) NOT NULL,
                sort_order  INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS menu_items (
                id          UUID PRIMARY KEY,
                category_id UUID REFERENCES categories(id),
                name        VARCHAR(100) NOT NULL,
                description TEXT,
                price       DECIMAL(10,2) NOT NULL,
                image_url   TEXT,
                is_available BOOLEAN DEFAULT TRUE,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_menu_category ON menu_items(category_id);
            CREATE INDEX IF NOT EXISTS idx_menu_available ON menu_items(is_available);
        """)
    logger.info("菜单数据库初始化完成")


async def publish(topic: str, event: dict):
    global kafka_producer
    try:
        if kafka_producer is None:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            await kafka_producer.start()
        await kafka_producer.send(topic, key=event["id"].encode(), value=event)
    except Exception as e:
        logger.error("事件发布失败", extra={"topic": topic, "error": str(e)})


# ============================================================
# FastAPI App
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    if db_pool:
        await db_pool.close()
    if kafka_producer:
        await kafka_producer.stop()

app = FastAPI(title="NekoCafé 菜单服务", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "service": "menu-service"}
    except Exception:
        raise HTTPException(503, "unhealthy")

@app.get("/readyz")
async def readyz():
    return {"status": "ready", "service": "menu-service"}


# -- 分类 API --

@app.post("/api/categories", status_code=201)
async def create_category(req: CategoryCreate):
    cat_id = str(uuid.uuid4())
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO categories (id, name, sort_order) VALUES ($1, $2, $3) RETURNING *",
            cat_id, req.name, req.sort_order,
        )
    await publish("menu.events", {"id": cat_id, "type": "CATEGORY_CREATED", "data": dict(row), "timestamp": datetime.utcnow().isoformat()})
    return dict(row)

@app.get("/api/categories")
async def list_categories():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM categories ORDER BY sort_order ASC")
    return {"data": [dict(r) for r in rows]}

@app.put("/api/categories/{category_id}")
async def update_category(category_id: str, req: CategoryUpdate):
    updates = {k: v for k, v in req.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(400, "无更新字段")
    set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates))
    params = list(updates.values()) + [category_id]
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(f"UPDATE categories SET {set_clause} WHERE id = ${len(params)} RETURNING *", *params)
    if not row:
        raise HTTPException(404, "分类不存在")
    return dict(row)

@app.delete("/api/categories/{category_id}")
async def delete_category(category_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("DELETE FROM categories WHERE id = $1 RETURNING *", category_id)
    if not row:
        raise HTTPException(404, "分类不存在")
    return {"message": "分类已删除"}


# -- 菜品 API --

@app.post("/api/menu-items", status_code=201)
async def create_menu_item(req: MenuItemCreate):
    item_id = str(uuid.uuid4())
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO menu_items (id, category_id, name, description, price, image_url, is_available)
               VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *""",
            item_id, req.category_id, req.name, req.description, req.price, req.image_url, req.is_available,
        )
    await publish("menu.events", {"id": item_id, "type": "MENU_ITEM_CREATED", "data": dict(row), "timestamp": datetime.utcnow().isoformat()})
    return _fmt_item(row)

@app.get("/api/menu-items")
async def list_menu_items(
    category_id: Optional[str] = Query(None),
    available_only: bool = Query(False),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
):
    conditions = []
    params = []
    idx = 1
    if category_id:
        conditions.append(f"category_id = ${idx}")
        params.append(category_id)
        idx += 1
    if available_only:
        conditions.append("is_available = TRUE")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM menu_items {where}", *params)
        offset = (page - 1) * limit
        rows = await conn.fetch(
            f"SELECT * FROM menu_items {where} ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *params, limit, offset,
        )
    return {
        "data": [_fmt_item(r) for r in rows],
        "pagination": {"page": page, "limit": limit, "total": total, "totalPages": (total + limit - 1) // limit},
    }

@app.get("/api/menu-items/{item_id}")
async def get_menu_item(item_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM menu_items WHERE id = $1", item_id)
    if not row:
        raise HTTPException(404, "菜品不存在")
    return _fmt_item(row)

@app.put("/api/menu-items/{item_id}")
async def update_menu_item(item_id: str, req: MenuItemUpdate):
    updates = {k: v for k, v in req.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(400, "无更新字段")
    updates["updated_at"] = datetime.utcnow()
    set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates))
    params = list(updates.values()) + [item_id]
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(f"UPDATE menu_items SET {set_clause} WHERE id = ${len(params)} RETURNING *", *params)
    if not row:
        raise HTTPException(404, "菜品不存在")
    await publish("menu.events", {"id": item_id, "type": "MENU_ITEM_UPDATED", "data": dict(row), "timestamp": datetime.utcnow().isoformat()})
    return _fmt_item(row)

@app.delete("/api/menu-items/{item_id}")
async def delete_menu_item(item_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("DELETE FROM menu_items WHERE id = $1 RETURNING *", item_id)
    if not row:
        raise HTTPException(404, "菜品不存在")
    return {"message": "菜品已删除"}


def _fmt_item(row):
    d = dict(row)
    d["price"] = float(d["price"])
    d["id"] = str(d["id"])
    d["category_id"] = str(d["category_id"]) if d.get("category_id") else None
    for k in ("created_at", "updated_at"):
        if k in d and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d
