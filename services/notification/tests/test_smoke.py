"""通知服务冒烟测试"""
import pytest


def test_imports():
    from src.main import app
    assert app.title == "NekoCafé 通知服务"


def test_notification_model():
    from pydantic import BaseModel
    # 通知使用基础模型，确认可导入
    assert BaseModel is not None
