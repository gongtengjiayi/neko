"""菜单服务冒烟测试"""
import pytest


def test_imports():
    from src.main import app
    assert app.title == "NekoCafé 菜单服务"


def test_category_price_model():
    from src.main import MenuItemCreate
    item = MenuItemCreate(category_id="0000-0000", name="拿铁", price=28.0)
    assert item.name == "拿铁"
    assert item.price == 28.0


def test_invalid_price():
    from src.main import MenuItemCreate
    with pytest.raises(ValueError):
        MenuItemCreate(category_id="0000-0000", name="免费", price=0)
