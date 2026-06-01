"""
NekoCafé 预约微服务 (Reservation Service)
- FastAPI 应用
- 预约 CRUD + Redis 分布式锁 + Kafka 事件发布
- CQRS 写模型
"""

import os
import uuid
import json
import asyncio
import logging
from datetime import datetime, date as date_type, time
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("reservation-service")

# ============================================================
# 配置（环境变量）
# ============================================================
DB_HOST = os.getenv("DB_HOST", "postgres-reservation")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "reservation")
DB_USER = os.getenv("DB_USER", "reservation")
DB_PASS = os.getenv("DB_PASS", "reservation123")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# ============================================================
# Pydantic 模型
# ============================================================

class CreateReservationRequest(BaseModel):
    member_id: str = Field(..., description="会员ID")
    date: date_type = Field(..., description="预约日期")
    time_slot: str = Field(..., pattern=r"^\d{2}:\d{2}$", description="时段 HH:MM")
    guest_count: int = Field(ge=1, le=20, description="人数 1-20")
    notes: Optional[str] = Field(None, max_length=200)
    contact_phone: str = Field(..., pattern=r"^1[3-9]\d{9}$")

    @field_validator("date")
    @classmethod
    def date_not_past(cls, v: date_type) -> date_type:
        if v < date_type.today():
            raise ValueError("预约日期不能是过去日期")
        return v

class UpdateReservationRequest(BaseModel):
    date: Optional[date_type] = None
    time_slot: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}$")
    guest_count: Optional[int] = Field(None, ge=1, le=20)
    notes: Optional[str] = Field(None, max_length=200)
    status: Optional[str] = Field(None, pattern=r"^(pending|confirmed|cancelled|completed)$")

class ReservationResponse(BaseModel):
    id: str
    member_id: str
    date: str
    time_slot: str
    guest_count: int
    status: str
    notes: Optional[str] = None
    contact_phone: str
    created_at: str
    updated_at: str

# ============================================================
# 全局客户端
# ============================================================
db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None
kafka_producer: Optional[AIOKafkaProducer] = None

# ============================================================
# 数据库初始化
# ============================================================
async def init_database():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASS,
        min_size=5, max_size=20,
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reservations (
                id              UUID PRIMARY KEY,
                member_id       UUID NOT NULL,
                date            DATE NOT NULL,
                time_slot       VARCHAR(5) NOT NULL,
                guest_count     INTEGER NOT NULL CHECK (guest_count >= 1 AND guest_count <= 20),
                status          VARCHAR(20) DEFAULT 'pending',
                notes           TEXT,
                contact_phone   VARCHAR(11) NOT NULL,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_reservations_date ON reservations(date);
            CREATE INDEX IF NOT EXISTS idx_reservations_member ON reservations(member_id);
            CREATE INDEX IF NOT EXISTS idx_reservations_status ON reservations(status);
        """)
    logger.info("预约数据库表初始化完成")

async def close_database():
    if db_pool:
        await db_pool.close()

# ============================================================
# Kafka 事件发布
# ============================================================
async def publish_event(topic: str, event: dict):
    global kafka_producer
    try:
        if kafka_producer is None:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            await kafka_producer.start()
        await kafka_producer.send(topic, key=event["id"].encode(), value=event)
        logger.info("事件已发布", extra={"topic": topic, "event_id": event["id"]})
    except Exception as e:
        logger.error("事件发布失败", extra={"topic": topic, "error": str(e)})

# ============================================================
# Redis 分布式锁（并发预约防护）
# ============================================================
LOCK_PREFIX = "reservation:lock:"
LOCK_TTL = 30  # 秒

async def acquire_lock(member_id: str, date_val: str, time_slot: str) -> bool:
    """尝试获取分布式锁，防止同一会员在同一时段重复预约"""
    lock_key = f"{LOCK_PREFIX}{member_id}:{date_val}:{time_slot}"
    result = await redis_client.setnx(lock_key, "1")
    if result:
        await redis_client.expire(lock_key, LOCK_TTL)
        return True
    return False

async def release_lock(member_id: str, date_val: str, time_slot: str):
    lock_key = f"{LOCK_PREFIX}{member_id}:{date_val}:{time_slot}"
    await redis_client.delete(lock_key)

# ============================================================
# FastAPI 应用
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    await init_database()
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Redis 已连接")
    yield
    await close_database()
    if kafka_producer:
        await kafka_producer.stop()
    if redis_client:
        await redis_client.close()
    logger.info("预约服务已关闭")

app = FastAPI(
    title="NekoCafé 预约服务",
    version="0.1.0",
    lifespan=lifespan,
)

# ============================================================
# 健康检查
# ============================================================

@app.get("/healthz")
async def healthz():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "service": "reservation-service", "timestamp": datetime.utcnow().isoformat()}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")

@app.get("/readyz")
async def readyz():
    return {"status": "ready", "service": "reservation-service"}

# ============================================================
# 预约 CRUD API
# ============================================================

@app.post("/api/reservations", status_code=201, response_model=ReservationResponse)
async def create_reservation(req: CreateReservationRequest):
    date_str = req.date.isoformat()

    # 分布式锁防重复
    locked = await acquire_lock(req.member_id, date_str, req.time_slot)
    if not locked:
        raise HTTPException(429, "该时段预约请求正在处理中，请稍后重试")

    try:
        # 检查时段容量
        async with db_pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM reservations WHERE date = $1 AND time_slot = $2 AND status IN ('pending', 'confirmed')",
                req.date, req.time_slot,
            )
            if existing and existing >= 10:
                raise HTTPException(409, "该时段预约已满")

            res_id = str(uuid.uuid4())
            now = datetime.utcnow()
            row = await conn.fetchrow(
                """INSERT INTO reservations (id, member_id, date, time_slot, guest_count, notes, contact_phone, created_at, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *""",
                res_id, req.member_id, req.date, req.time_slot, req.guest_count,
                req.notes or None, req.contact_phone, now, now,
            )
    finally:
        await release_lock(req.member_id, date_str, req.time_slot)

    await publish_event("reservation.events", {
        "id": res_id,
        "type": "RESERVATION_CREATED",
        "data": dict(row),
        "timestamp": now.isoformat(),
    })

    return _format_reservation(row)

@app.get("/api/reservations", response_model=dict)
async def list_reservations(
    date: Optional[str] = Query(None),
    member_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conditions = []
    params = []
    idx = 1

    if date:
        conditions.append(f"date = ${idx}")
        params.append(date)
        idx += 1
    if member_id:
        conditions.append(f"member_id = ${idx}")
        params.append(member_id)
        idx += 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM reservations {where_clause}", *params)
        offset = (page - 1) * limit
        rows = await conn.fetch(
            f"SELECT * FROM reservations {where_clause} ORDER BY date DESC, time_slot ASC LIMIT ${idx} OFFSET ${idx + 1}",
            *params, limit, offset,
        )

    return {
        "data": [_format_reservation(r) for r in rows],
        "pagination": {"page": page, "limit": limit, "total": total, "totalPages": (total + limit - 1) // limit},
    }

@app.get("/api/reservations/{reservation_id}", response_model=ReservationResponse)
async def get_reservation(reservation_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM reservations WHERE id = $1", reservation_id)
    if not row:
        raise HTTPException(404, "预约不存在")
    return _format_reservation(row)

@app.put("/api/reservations/{reservation_id}", response_model=ReservationResponse)
async def update_reservation(reservation_id: str, req: UpdateReservationRequest):
    updates = {}
    for field, val in req.model_dump(exclude_none=True).items():
        if val is not None:
            updates[field] = val

    if not updates:
        raise HTTPException(400, "未提供任何更新字段")

    updates["updated_at"] = datetime.utcnow()
    set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates))
    params = list(updates.values()) + [reservation_id]

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE reservations SET {set_clause} WHERE id = ${len(params)} RETURNING *",
            *params,
        )
    if not row:
        raise HTTPException(404, "预约不存在")

    await publish_event("reservation.events", {
        "id": reservation_id,
        "type": "RESERVATION_UPDATED",
        "data": dict(row),
        "timestamp": updates["updated_at"].isoformat(),
    })

    return _format_reservation(row)

@app.delete("/api/reservations/{reservation_id}")
async def cancel_reservation(reservation_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE reservations SET status = 'cancelled', updated_at = $1 WHERE id = $2 RETURNING *",
            datetime.utcnow(), reservation_id,
        )
    if not row:
        raise HTTPException(404, "预约不存在")

    await publish_event("reservation.events", {
        "id": reservation_id,
        "type": "RESERVATION_CANCELLED",
        "data": dict(row),
        "timestamp": datetime.utcnow().isoformat(),
    })

    return {"message": "预约已取消", "reservation": _format_reservation(row)}

# ============================================================
# 辅助函数
# ============================================================

def _format_reservation(row) -> dict:
    return {
        "id": str(row["id"]),
        "member_id": str(row["member_id"]),
        "date": row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"]),
        "time_slot": row["time_slot"],
        "guest_count": row["guest_count"],
        "status": row["status"],
        "notes": row["notes"],
        "contact_phone": row["contact_phone"],
        "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
        "updated_at": row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else str(row["updated_at"]),
    }
