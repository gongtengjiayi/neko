# NekoCafé

> NekoCafé 智慧餐饮预约平台 — 实验三 PoC 仓库  
> 6 微服务 + API 网关 + Kafka 事件总线 + CQRS

## 架构总览

```
gateway (nginx:8080)
  ├── reservation  (FastAPI :8081) → PostgreSQL + Redis + Kafka
  ├── member       (Express :8082) → PostgreSQL + Kafka
  ├── menu         (FastAPI :8083) → PostgreSQL + Kafka
  ├── order        (FastAPI :8084) → PostgreSQL + Kafka
  ├── payment      (FastAPI :8085) → PostgreSQL + Kafka
  └── notification (FastAPI :8086) → PostgreSQL + Kafka (Consumer)
```

## 一键启动

```bash
make up      # 构建并启动全部服务
```

所有服务启动后，访问网关：http://localhost:8080

## 各服务端口

| 服务 | 端口 | 技术栈 |
|------|------|--------|
| API 网关 | 8080 | Nginx |
| 预约 (reservation) | 8081 | Python FastAPI |
| 会员 (member) | 8082 | Node.js Express |
| 菜单 (menu) | 8083 | Python FastAPI |
| 订单 (order) | 8084 | Python FastAPI |
| 支付 (payment) | 8085 | Python FastAPI |
| 通知 (notification) | 8086 | Python FastAPI |
| Redis | 6379 | Redis 7 |
| Kafka | 9092 | Kafka 7.7 |

## 常用命令

```bash
make up      # 启动所有服务
make down    # 停止并清理数据卷
make logs    # 查看实时日志
make test    # 运行测试
make ps      # 查看服务状态
make clean   # 清理所有镜像和数据
```

## 验证

```bash
# 网关健康检查
curl http://localhost:8080/healthz

# 预约服务
curl http://localhost:8081/healthz

# 会员服务
curl http://localhost:8082/healthz
```

## 项目结构

```
.
├── gateway/
│   └── nginx.conf          # API 网关配置
├── services/
│   ├── member/             # 会员服务 (Node.js)
│   ├── reservation/        # 预约服务 (Python)
│   ├── menu/               # 菜单服务 (Python)
│   ├── order/              # 订单服务 (Python)
│   ├── payment/            # 支付服务 (Python)
│   └── notification/       # 通知服务 (Python)
├── .github/workflows/
│   ├── ci.yml              # CI 流水线
│   └── cd.yml              # CD 流水线
├── docs/
│   ├── rollback.md         # 回滚指南
│   └── runbook.md          # 运维手册
├── docker-compose.yml      # 本地编排
├── Makefile                # 快捷命令
└── README.md               # 本文件
```

## 环境变量

各微服务的数据库连接、Kafka 地址等配置通过环境变量注入，详见 `docker-compose.yml` 各 service 的 `environment` 段。
