"""
NekoCafé 通知微服务 (Notification Service)
- 消费 Kafka 事件并发送通知
- 支持短信 / 邮件 / 站内信
- 通知记录查询
"""

import os, uuid, json, asyncio, logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("notification-service")

DB_HOST = os.getenv("DB_HOST", "postgres-notification")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "notification")
DB_USER = os.getenv("DB_USER", "notification")
DB_PASS = os.getenv("DB_PASS", "notification123")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

db_pool: Optional[asyncpg.Pool] = None


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=3, max_size=10,
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id          UUID PRIMARY KEY,
                recipient   VARCHAR(100) NOT NULL,
                channel     VARCHAR(20) NOT NULL,
                title       VARCHAR(200),
                content     TEXT NOT NULL,
                status      VARCHAR(20) DEFAULT 'pending',
                sent_at     TIMESTAMP,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_notifications_recipient ON notifications(recipient);
            CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status);
        """)
    logger.info("通知数据库初始化完成")


async def send_notification(recipient: str, channel: str, title: str, content: str) -> str:
    """模拟发送通知（短信/邮件/站内信），返回通知 ID"""
    notif_id = str(uuid.uuid4())
    now = datetime.utcnow()

    # 模拟发送延迟
    await asyncio.sleep(0.1)

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO notifications (id, recipient, channel, title, content, status, sent_at) VALUES ($1,$2,$3,$4,$5,'sent',$6)",
            notif_id, recipient, channel, title, content, now,
        )
    logger.info(f"通知已发送: channel={channel} recipient={recipient} title={title}")
    return notif_id


async def consume_events():
    """后台任务：消费 Kafka 事件并根据事件类型发送通知"""
    try:
        consumer = AIOKafkaConsumer(
            "member.events", "reservation.events", "order.events", "payment.events",
            bootstrap_servers=KAFKA_BROKER,
            group_id="notification-service",
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
        )
        await consumer.start()

        logger.info("Kafka 消费者已启动，正在监听事件...")
        async for msg in consumer:
            try:
                event = msg.value
                event_type = event.get("type", "")
                data = event.get("data", {})
                logger.info(f"收到事件: topic={msg.topic} type={event_type}")

                if event_type == "RESERVATION_CREATED":
                    await send_notification(
                        data.get("contact_phone", ""), "sms",
                        "预约确认",
                        f"您已成功预约 NekoCafé！预约编号: {data.get('id', '')[:8]}",
                    )
                elif event_type == "RESERVATION_CANCELLED":
                    await send_notification(
                        data.get("contact_phone", ""), "sms",
                        "预约取消",
                        f"您的预约已取消。预约编号: {data.get('id', '')[:8]}",
                    )
                elif event_type == "ORDER_CREATED":
                    order = data.get("order", {})
                    await send_notification(
                        order.get("member_id", ""), "in_app",
                        "新订单已创建",
                        f"订单金额: ¥{order.get('total_amount', 0):.2f}",
                    )
                elif event_type == "PAYMENT_SUCCESS":
                    await send_notification(
                        data.get("member_id", ""), "in_app",
                        "支付成功",
                        f"支付金额: ¥{data.get('amount', 0):.2f}",
                    )
                elif event_type == "PAYMENT_REFUNDED":
                    await send_notification(
                        data.get("member_id", ""), "in_app",
                        "退款成功",
                        f"退款金额: ¥{data.get('amount', 0):.2f}",
                    )
            except Exception as e:
                logger.error(f"处理事件失败", extra={"error": str(e)})

    except Exception as e:
        logger.error(f"Kafka 消费者异常", extra={"error": str(e)})


consumer_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    await init_db()
    consumer_task = asyncio.create_task(consume_events())
    yield
    if consumer_task:
        consumer_task.cancel()
    if db_pool:
        await db_pool.close()

app = FastAPI(title="NekoCafé 通知服务", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "service": "notification-service"}
    except Exception:
        raise HTTPException(503, "unhealthy")

@app.get("/readyz")
async def readyz():
    return {"status": "ready", "service": "notification-service"}


@app.post("/api/notifications/send", status_code=201)
async def send_notification_api(recipient: str, channel: str = "sms", title: str = "", content: str = ""):
    """手动发送通知"""
    notif_id = await send_notification(recipient, channel, title, content)
    return {"id": notif_id, "status": "sent"}


@app.get("/api/notifications")
async def list_notifications(
    recipient: Optional[str] = Query(None),
    channel: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conditions, params = [], []
    idx = 1
    if recipient:
        conditions.append(f"recipient = ${idx}"); params.append(recipient); idx += 1
    if channel:
        conditions.append(f"channel = ${idx}"); params.append(channel); idx += 1
    if status:
        conditions.append(f"status = ${idx}"); params.append(status); idx += 1
    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM notifications {where}", *params)
        offset = (page - 1) * limit
        rows = await conn.fetch(
            f"SELECT * FROM notifications {where} ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *params, limit, offset,
        )
    return {
        "data": [_fmt_notif(r) for r in rows],
        "pagination": {"page": page, "limit": limit, "total": total, "totalPages": (total + limit - 1) // limit},
    }


@app.get("/api/notifications/{notification_id}")
async def get_notification(notification_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM notifications WHERE id = $1", notification_id)
    if not row:
        raise HTTPException(404, "通知不存在")
    return _fmt_notif(row)


def _fmt_notif(row):
    d = dict(row)
    d["id"] = str(d["id"])
    for k in ("created_at", "sent_at"):
        if k in d and d[k] and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d
