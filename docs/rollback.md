# Rollback — 回滚指南

## 一键回滚

### 单服务回滚（Helm）

```bash
# 回滚到上一版本
helm rollback nekocafe-reservation -n prod

# 回滚到指定版本
helm history nekocafe-reservation -n prod
helm rollback nekocafe-reservation <REVISION> -n prod
```

### 数据库回滚（Flyway）

```bash
# staging 环境可自动回滚
flyway undo

# prod 环境需人工介入评估数据影响
# 1. 检查受影响数据
# 2. 执行逆向迁移脚本
# 3. 验证数据一致性
```

## 自动回滚触发条件

| 条件 | 阈值 | 动作 |
|------|------|------|
| HTTP 5xx 错误率 | > 5% 持续 2 分钟 | 自动回滚 + PagerDuty 告警 |
| P95 延迟 | > 500ms 持续 3 分钟 | 自动回滚 + 告警 |
| 健康检查 | 连续 3 次失败 | 立即回滚 |
| 金丝雀监控 | 指标异常 | Flagger 自动缩回 |

## 回滚后操作

1. 保留失败版本 Pod 24 小时用于事后分析
2. 在 GitHub 创建 Incident Report Issue
3. 锁定后续部署直到 RCA 完成

## 本地开发回滚

```bash
# Docker Compose 回滚
git revert HEAD
make up
```
