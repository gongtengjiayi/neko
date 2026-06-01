.PHONY: up down test logs ps clean

up:
	docker compose up -d --build
	@echo "✅ 所有服务已启动"
	@echo "   网关: http://localhost:8080"
	@echo "   预约: http://localhost:8081"
	@echo "   会员: http://localhost:8082"
	@echo "   菜单: http://localhost:8083"
	@echo "   订单: http://localhost:8084"
	@echo "   支付: http://localhost:8085"
	@echo "   通知: http://localhost:8086"

down:
	docker compose down -v
	@echo "✅ 所有服务已停止"

test:
	docker compose exec reservation python -m pytest -v
	- docker compose exec member sh -c "npm test" 2>nul

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v --rmi all
	docker system prune -f
