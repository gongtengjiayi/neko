"""支付服务冒烟测试"""
import pytest


def test_imports():
    from src.main import app
    assert app.title == "NekoCafé 支付服务"


def test_payment_model():
    from src.main import CreatePaymentRequest
    req = CreatePaymentRequest(order_id="o1", member_id="m1", amount=100.0, method="alipay")
    assert req.method == "alipay"

    with pytest.raises(ValueError):
        CreatePaymentRequest(order_id="o1", member_id="m1", amount=100.0, method="bitcoin")
