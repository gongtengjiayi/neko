const express = require('express');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const Joi = require('joi');
const { createLogger, format, transports } = require('winston');

// ============================================================
// 配置
// ============================================================
const PORT = process.env.PORT || 8080;
const DB_HOST = process.env.DB_HOST || 'postgres-member';
const DB_PORT = parseInt(process.env.DB_PORT || '5432', 10);
const DB_NAME = process.env.DB_NAME || 'member';
const DB_USER = process.env.DB_USER || 'member';
const DB_PASS = process.env.DB_PASS || 'member123';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';

// ============================================================
// 日志
// ============================================================
const logger = createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: format.combine(
    format.timestamp(),
    format.json()
  ),
  defaultMeta: { service: 'member-service' },
  transports: [new transports.Console()],
});

// ============================================================
// 数据库连接池
// ============================================================
const pool = new Pool({
  host: DB_HOST,
  port: DB_PORT,
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASS,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// ============================================================
// Kafka 生产者
// ============================================================
const kafka = new Kafka({
  clientId: 'member-service',
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 1000, retries: 5 },
});

const producer = kafka.producer();

async function publishEvent(topic, event) {
  try {
    await producer.send({
      topic,
      messages: [{ key: event.id, value: JSON.stringify(event) }],
    });
    logger.info('事件已发布', { topic, eventId: event.id });
  } catch (err) {
    logger.error('事件发布失败', { topic, error: err.message });
  }
}

// ============================================================
// Joi 校验 Schema
// ============================================================
const createMemberSchema = Joi.object({
  name: Joi.string().min(1).max(50).required(),
  phone: Joi.string().pattern(/^1[3-9]\d{9}$/).required(),
  email: Joi.string().email().allow('', null),
  avatar: Joi.string().uri().allow('', null),
});

const updateMemberSchema = Joi.object({
  name: Joi.string().min(1).max(50),
  phone: Joi.string().pattern(/^1[3-9]\d{9}$/),
  email: Joi.string().email().allow('', null),
  avatar: Joi.string().uri().allow('', null),
  points: Joi.number().integer().min(0),
  level: Joi.string().valid('bronze', 'silver', 'gold', 'platinum'),
}).min(1);

// ============================================================
// 数据库初始化
// ============================================================
async function initDatabase() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS members (
        id          UUID PRIMARY KEY,
        name        VARCHAR(50) NOT NULL,
        phone       VARCHAR(11) UNIQUE NOT NULL,
        email       VARCHAR(100),
        avatar      TEXT,
        points      INTEGER DEFAULT 0,
        level       VARCHAR(20) DEFAULT 'bronze',
        status      VARCHAR(20) DEFAULT 'active',
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_members_phone ON members(phone);
      CREATE INDEX IF NOT EXISTS idx_members_level ON members(level);
    `);
    logger.info('数据库表初始化完成');
  } finally {
    client.release();
  }
}

// ============================================================
// Express 应用
// ============================================================
const app = express();
app.use(express.json());

// 健康检查
app.get('/healthz', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', service: 'member-service', timestamp: new Date().toISOString() });
  } catch {
    res.status(503).json({ status: 'unhealthy', service: 'member-service' });
  }
});

// 就绪检查
app.get('/readyz', (_req, res) => {
  res.json({ status: 'ready', service: 'member-service' });
});

// ============================================================
// 会员 CRUD API
// ============================================================

// POST /api/members — 创建会员
app.post('/api/members', async (req, res) => {
  try {
    const { error, value } = createMemberSchema.validate(req.body);
    if (error) return res.status(400).json({ error: error.details[0].message });

    const id = uuidv4();
    const result = await pool.query(
      `INSERT INTO members (id, name, phone, email, avatar)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [id, value.name, value.phone, value.email || null, value.avatar || null]
    );

    const member = result.rows[0];
    await publishEvent('member.events', {
      id: member.id,
      type: 'MEMBER_CREATED',
      data: member,
      timestamp: new Date().toISOString(),
    });

    res.status(201).json(member);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: '手机号已注册' });
    }
    logger.error('创建会员失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// GET /api/members — 获取会员列表
app.get('/api/members', async (req, res) => {
  try {
    const page = parseInt(req.query.page || '1', 10);
    const limit = parseInt(req.query.limit || '20', 10);
    const offset = (page - 1) * limit;

    const countResult = await pool.query('SELECT COUNT(*) FROM members');
    const total = parseInt(countResult.rows[0].count, 10);

    const result = await pool.query(
      'SELECT * FROM members ORDER BY created_at DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );

    res.json({
      data: result.rows,
      pagination: { page, limit, total, totalPages: Math.ceil(total / limit) },
    });
  } catch (err) {
    logger.error('查询会员列表失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// GET /api/members/:id — 获取单个会员
app.get('/api/members/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM members WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ error: '会员不存在' });
    res.json(result.rows[0]);
  } catch (err) {
    logger.error('查询会员失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// PUT /api/members/:id — 更新会员
app.put('/api/members/:id', async (req, res) => {
  try {
    const { error, value } = updateMemberSchema.validate(req.body);
    if (error) return res.status(400).json({ error: error.details[0].message });

    const sets = [];
    const params = [];
    let idx = 1;
    for (const [key, val] of Object.entries(value)) {
      sets.push(`${key} = $${idx++}`);
      params.push(val);
    }
    sets.push(`updated_at = CURRENT_TIMESTAMP`);
    params.push(req.params.id);

    const result = await pool.query(
      `UPDATE members SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
      params
    );
    if (result.rows.length === 0) return res.status(404).json({ error: '会员不存在' });

    await publishEvent('member.events', {
      id: result.rows[0].id,
      type: 'MEMBER_UPDATED',
      data: result.rows[0],
      timestamp: new Date().toISOString(),
    });

    res.json(result.rows[0]);
  } catch (err) {
    logger.error('更新会员失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// DELETE /api/members/:id — 删除会员（软删除）
app.delete('/api/members/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `UPDATE members SET status = 'inactive', updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: '会员不存在' });

    await publishEvent('member.events', {
      id: result.rows[0].id,
      type: 'MEMBER_DEACTIVATED',
      data: { id: result.rows[0].id },
      timestamp: new Date().toISOString(),
    });

    res.json({ message: '会员已停用' });
  } catch (err) {
    logger.error('停用会员失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// POST /api/members/:id/points — 积分操作
app.post('/api/members/:id/points', async (req, res) => {
  try {
    const { amount, reason } = req.body;
    if (!amount || typeof amount !== 'number') {
      return res.status(400).json({ error: '积分变动值 amount 必填且为数字' });
    }

    const result = await pool.query(
      `UPDATE members SET points = points + $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING *`,
      [amount, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: '会员不存在' });

    await publishEvent('member.events', {
      id: result.rows[0].id,
      type: 'POINTS_CHANGED',
      data: { memberId: result.rows[0].id, amount, reason, balance: result.rows[0].points },
      timestamp: new Date().toISOString(),
    });

    res.json(result.rows[0]);
  } catch (err) {
    logger.error('积分操作失败', { error: err.message });
    res.status(500).json({ error: '内部服务器错误' });
  }
});

// ============================================================
// 启动
// ============================================================
async function start() {
  try {
    await initDatabase();
    await producer.connect();
    logger.info('Kafka 生产者已连接');

    app.listen(PORT, () => {
      logger.info(`会员服务已启动，端口: ${PORT}`);
    });
  } catch (err) {
    logger.error('服务启动失败', { error: err.message });
    process.exit(1);
  }
}

// 优雅关闭
process.on('SIGTERM', async () => {
  logger.info('收到 SIGTERM，正在关闭...');
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('收到 SIGINT，正在关闭...');
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

if (require.main === module) {
  start();
}

module.exports = app;
