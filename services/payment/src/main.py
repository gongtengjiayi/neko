"""
NekoCafé 支付微服务 (Payment Service)
- 支付记录创建与状态查询
- 模拟支付网关对接
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
logger = logging.getLogger("payment-service")

DB_HOST = os.getenv("DB_HOST", "postgres-payment")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "payment")
DB_USER = os.getenv("DB_USER", "payment")
DB_PASS = os.getenv("DB_PASS", "payment123")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

db_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[AIOKafkaProducer] = None

PAYMENT_METHODS = ("alipay", "wechat", "card")
PAYMENT_STATUS = ("pending", "processing", "success", "failed", "refunded")


class CreatePaymentRequest(BaseModel):
    order_id: str
    member_id: str
    amount: float = Field(gt=0)
    method: str = Field(..., pattern=r"^(alipay|wechat|card)$")


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=3, max_size=10,
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id          UUID PRIMARY KEY,
                order_id    UUID NOT NULL,
                member_id   UUID NOT NULL,
                amount      DECIMAL(10,2) NOT NULL,
                method      VARCHAR(20) NOT NULL,
                status      VARCHAR(20) DEFAULT 'pending',
                paid_at     TIMESTAMP,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
            CREATE INDEX IF NOT EXISTS idx_payments_member ON payments(member_id);
        """)
    logger.info("支付数据库初始化完成")


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

app = FastAPI(title="NekoCafé 支付服务", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "service": "payment-service"}
    except Exception:
        raise HTTPException(503, "unhealthy")

@app.get("/readyz")
async def readyz():
    return {"status": "ready", "service": "payment-service"}


@app.post("/api/payments", status_code=201)
async def create_payment(req: CreatePaymentRequest):
    pay_id = str(uuid.uuid4())
    now = datetime.utcnow()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO payments (id, order_id, member_id, amount, method, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *",
            pay_id, req.order_id, req.member_id, req.amount, req.method, now, now,
        )

    await publish("payment.events", {
        "id": pay_id, "type": "PAYMENT_CREATED",
        "data": dict(row), "timestamp": now.isoformat(),
    })
    return _fmt_payment(row)


@app.post("/api/payments/{payment_id}/process")
async def process_payment(payment_id: str):
    """模拟支付处理：pending → processing → success"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
        if not row:
            raise HTTPException(404, "支付记录不存在")
        if row["status"] != "pending":
            raise HTTPException(400, f"当前状态不允许处理: {row['status']}")

        # 模拟支付网关处理
        now = datetime.utcnow()
        row = await conn.fetchrow(
            "UPDATE payments SET status = 'success', paid_at = $1, updated_at = $2 WHERE id = $3 RETURNING *",
            now, now, payment_id,
        )

    await publish("payment.events", {
        "id": payment_id, "type": "PAYMENT_SUCCESS",
        "data": dict(row), "timestamp": datetime.utcnow().isoformat(),
    })
    return _fmt_payment(row)


@app.post("/api/payments/{payment_id}/refund")
async def refund_payment(payment_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
        if not row:
            raise HTTPException(404, "支付记录不存在")
        if row["status"] != "success":
            raise HTTPException(400, "仅成功支付可退款")

        row = await conn.fetchrow(
            "UPDATE payments SET status = 'refunded', updated_at = $1 WHERE id = $2 RETURNING *",
            datetime.utcnow(), payment_id,
        )

    await publish("payment.events", {
        "id": payment_id, "type": "PAYMENT_REFUNDED",
        "data": dict(row), "timestamp": datetime.utcnow().isoformat(),
    })
    return _fmt_payment(row)


@app.get("/api/payments")
async def list_payments(
    member_id: Optional[str] = Query(None),
    order_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conditions, params = [], []
    idx = 1
    if member_id:
        conditions.append(f"member_id = ${idx}"); params.append(member_id); idx += 1
    if order_id:
        conditions.append(f"order_id = ${idx}"); params.append(order_id); idx += 1
    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM payments {where}", *params)
        offset = (page - 1) * limit
        rows = await conn.fetch(
            f"SELECT * FROM payments {where} ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *params, limit, offset,
        )
    return {
        "data": [_fmt_payment(r) for r in rows],
        "pagination": {"page": page, "limit": limit, "total": total, "totalPages": (total + limit - 1) // limit},
    }


@app.get("/api/payments/{payment_id}")
async def get_payment(payment_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
    if not row:
        raise HTTPException(404, "支付记录不存在")
    return _fmt_payment(row)


def _fmt_payment(row):
    d = dict(row)
    d["amount"] = float(d["amount"])
    for k in ("id", "order_id", "member_id"):
        d[k] = str(d[k])
    for k in ("created_at", "updated_at", "paid_at"):
        if k in d and d[k] and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d
