"""订单服务冒烟测试"""
import pytest


def test_imports():
    from src.main import app
    assert app.title == "NekoCafé 订单服务"


def test_order_item_model():
    from src.main import OrderItem, CreateOrderRequest
    item = OrderItem(menu_item_id="m1", name="拿铁", quantity=2, unit_price=28.0)
    assert item.subtotal is None  # computed later

    with pytest.raises(ValueError):
        OrderItem(menu_item_id="m1", name="测试", quantity=0, unit_price=28.0)
