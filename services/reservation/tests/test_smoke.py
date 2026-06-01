"""预约服务冒烟测试"""

import pytest
from datetime import date, datetime


def test_imports():
    """验证核心依赖可导入"""
    import fastapi
    import asyncpg
    import redis.asyncio
    import aiokafka
    import pydantic
    assert fastapi.__version__


def test_date_validation():
    """测试预约日期不能是过去"""
    from src.main import CreateReservationRequest
    import uuid

    today = date.today()
    # 今天应该通过
    req = CreateReservationRequest(
        member_id=str(uuid.uuid4()),
        date=today,
        time_slot="12:00",
        guest_count=2,
        contact_phone="13800138000",
    )
    assert req.date == today

    # 过去日期应该失败
    past = date(2020, 1, 1)
    with pytest.raises(ValueError, match="不能是过去日期"):
        CreateReservationRequest(
            member_id=str(uuid.uuid4()),
            date=past,
            time_slot="12:00",
            guest_count=2,
            contact_phone="13800138000",
        )


def test_guest_count_range():
    """测试人数范围 1-20"""
    from src.main import CreateReservationRequest
    import uuid

    today = date.today()
    # 边界值
    req = CreateReservationRequest(
        member_id=str(uuid.uuid4()),
        date=today,
        time_slot="18:00",
        guest_count=20,
        contact_phone="13900139000",
    )
    assert req.guest_count == 20

    # 超过 20 应该失败
    with pytest.raises(ValueError):
        CreateReservationRequest(
            member_id=str(uuid.uuid4()),
            date=today,
            time_slot="18:00",
            guest_count=21,
            contact_phone="13900139000",
        )


def test_phone_format():
    """测试手机号格式"""
    from src.main import CreateReservationRequest
    import uuid

    today = date.today()
    with pytest.raises(ValueError):
        CreateReservationRequest(
            member_id=str(uuid.uuid4()),
            date=today,
            time_slot="12:00",
            guest_count=2,
            contact_phone="12345",
        )
