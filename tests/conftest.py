import pytest_asyncio
from httpx import AsyncClient
from src.main import app
from src.db.mongo import get_db


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db():
    return await get_db()
