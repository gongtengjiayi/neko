"""
NekoCafé 订单微服务 (Order Service)
- 订单创建/查询/状态流转
- Kafka 事件发布
"""

import os, uuid, json, logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("order-service")

DB_HOST = os.getenv("DB_HOST", "postgres-order")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "order")
DB_USER = os.getenv("DB_USER", "order")
DB_PASS = os.getenv("DB_PASS", "order123")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

db_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[AIOKafkaProducer] = None


class OrderItem(BaseModel):
    menu_item_id: str
    name: str
    quantity: int = Field(ge=1, le=50)
    unit_price: float = Field(gt=0)

class CreateOrderRequest(BaseModel):
    member_id: str
    reservation_id: Optional[str] = None
    items: list[OrderItem] = Field(..., min_length=1)
    notes: Optional[str] = None

class UpdateOrderStatusRequest(BaseModel):
    status: str = Field(..., pattern=r"^(pending|confirmed|preparing|served|completed|cancelled)$")


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=3, max_size=10,
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              UUID PRIMARY KEY,
                member_id       UUID NOT NULL,
                reservation_id  UUID,
                status          VARCHAR(20) DEFAULT 'pending',
                total_amount    DECIMAL(10,2) DEFAULT 0,
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS order_items (
                id          UUID PRIMARY KEY,
                order_id    UUID REFERENCES orders(id) ON DELETE CASCADE,
                menu_item_id UUID NOT NULL,
                name        VARCHAR(100) NOT NULL,
                quantity    INTEGER NOT NULL,
                unit_price  DECIMAL(10,2) NOT NULL,
                subtotal    DECIMAL(10,2) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_orders_member ON orders(member_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
        """)
    logger.info("订单数据库初始化完成")


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    if db_pool: await db_pool.close()
    if kafka_producer: await kafka_producer.stop()

app = FastAPI(title="NekoCafé 订单服务", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "service": "order-service"}
    except Exception:
        raise HTTPException(503, "unhealthy")

@app.get("/readyz")
async def readyz():
    return {"status": "ready", "service": "order-service"}


@app.post("/api/orders", status_code=201)
async def create_order(req: CreateOrderRequest):
    order_id = str(uuid.uuid4())
    total = sum(item.unit_price * item.quantity for item in req.items)
    now = datetime.utcnow()

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO orders (id, member_id, reservation_id, total_amount, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                order_id, req.member_id, req.reservation_id, total, req.notes, now, now,
            )
            for item in req.items:
                item_id = str(uuid.uuid4())
                subtotal = item.unit_price * item.quantity
                await conn.execute(
                    "INSERT INTO order_items (id, order_id, menu_item_id, name, quantity, unit_price, subtotal) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    item_id, order_id, item.menu_item_id, item.name, item.quantity, item.unit_price, subtotal,
                )

        order = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
        items = await conn.fetch("SELECT * FROM order_items WHERE order_id = $1", order_id)

    await publish("order.events", {
        "id": order_id, "type": "ORDER_CREATED",
        "data": {"order": dict(order), "items": [dict(i) for i in items]},
        "timestamp": now.isoformat(),
    })

    return _fmt_order(order, items)


@app.get("/api/orders")
async def list_orders(
    member_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conditions, params = [], []
    idx = 1
    if member_id:
        conditions.append(f"o.member_id = ${idx}"); params.append(member_id); idx += 1
    if status:
        conditions.append(f"o.status = ${idx}"); params.append(status); idx += 1
    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM orders o {where}", *params)
        offset = (page - 1) * limit
        rows = await conn.fetch(
            f"SELECT o.* FROM orders o {where} ORDER BY o.created_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *params, limit, offset,
        )
        result = []
        for r in rows:
            items = await conn.fetch("SELECT * FROM order_items WHERE order_id = $1", r["id"])
            result.append(_fmt_order(r, items))

    return {"data": result, "pagination": {"page": page, "limit": limit, "total": total, "totalPages": (total + limit - 1) // limit}}


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    async with db_pool.acquire() as conn:
        order = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
        if not order:
            raise HTTPException(404, "订单不存在")
        items = await conn.fetch("SELECT * FROM order_items WHERE order_id = $1", order_id)
    return _fmt_order(order, items)


@app.put("/api/orders/{order_id}/status")
async def update_order_status(order_id: str, req: UpdateOrderStatusRequest):
    now = datetime.utcnow()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3 RETURNING *",
            req.status, now, order_id,
        )
        if not row:
            raise HTTPException(404, "订单不存在")
        items = await conn.fetch("SELECT * FROM order_items WHERE order_id = $1", order_id)

    await publish("order.events", {
        "id": order_id, "type": "ORDER_STATUS_CHANGED",
        "data": {"order_id": order_id, "status": req.status},
        "timestamp": now.isoformat(),
    })
    return _fmt_order(row, items)


def _fmt_order(order, items):
    o = dict(order)
    o["id"] = str(o["id"])
    o["member_id"] = str(o["member_id"])
    o["total_amount"] = float(o["total_amount"])
    for k in ("created_at", "updated_at"):
        if k in o and hasattr(o[k], "isoformat"):
            o[k] = o[k].isoformat()
    o["items"] = []
    for item in items:
        i = dict(item)
        i["unit_price"] = float(i["unit_price"])
        i["subtotal"] = float(i["subtotal"])
        for k in ("id", "order_id", "menu_item_id"):
            i[k] = str(i[k])
        o["items"].append(i)
    return o
