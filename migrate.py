import asyncio
from models import init_orm


async def create_tables():
    await init_orm()
    print("Таблицы успешно созданы!")


if __name__ == "__main__":
    asyncio.run(create_tables())