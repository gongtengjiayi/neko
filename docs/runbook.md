# Runbook — 运维手册

## 服务异常告警时

1. **检查 Grafana 仪表盘**
   - 查看 P99/P95 延迟趋势
   - 查看 5xx 错误率
   - 查看 CPU/内存使用率

2. **日志检索 (Loki)**
   ```bash
   # 查询最近 5 分钟 ERROR 日志
   {service="reservation-service"} |= "ERROR" |= "5m"
   ```

3. **分布式追踪 (Tempo)**
   - 从 Grafana Explore 选择 Tempo 数据源
   - 按 service.name 和 http.status_code 过滤
   - 找出异常的 traceId 定位根因

## 服务启动顺序

```bash
# 基础设施优先
docker compose up -d zookeeper kafka redis
# 等待 Kafka Ready
# 数据库
docker compose up -d postgres-*
# 微服务
docker compose up -d reservation member menu order payment notification
# 网关
docker compose up -d gateway
```

## Kafka Topic 列表

| Topic | 生产者 | 消费者 | 事件类型 |
|-------|--------|--------|----------|
| member.events | member | notification | MEMBER_CREATED, MEMBER_UPDATED, POINTS_CHANGED |
| reservation.events | reservation | notification | RESERVATION_CREATED, RESERVATION_CANCELLED |
| menu.events | menu | - | CATEGORY_CREATED, MENU_ITEM_CREATED |
| order.events | order | notification | ORDER_CREATED, ORDER_STATUS_CHANGED |
| payment.events | payment | notification | PAYMENT_CREATED, PAYMENT_SUCCESS, PAYMENT_REFUNDED |

## 数据库连接

| 服务 | 数据库 | 端口映射 |
|------|--------|----------|
| member | postgres-member:5432 | - |
| reservation | postgres-reservation:5432 | - |
| menu | postgres-menu:5432 | - |
| order | postgres-order:5432 | - |
| payment | postgres-payment:5432 | - |
| notification | postgres-notification:5432 | - |

## 健康检查端点

- `GET /healthz` — 存活检查 (含 DB 连通性)
- `GET /readyz` — 就绪检查

## PagerDuty 告警级别

| 级别 | 响应时间 | 通知方式 |
|------|----------|----------|
| P0 Critical | 15 分钟 | 电话 + 短信 + Slack |
| P1 Warning | 30 分钟 | 短信 + Slack |
| P2 Info | 4 小时 | Slack 通知 |
