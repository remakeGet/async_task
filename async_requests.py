import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import DbSession, SwapiPeople, close_orm, init_orm

MAX_REQUESTS = 10
API_BASE_URL = "https://www.swapi.tech/api/people/"


async def get_people(person_id, http_session):
    try:
        async with http_session.get(f"{API_BASE_URL}{person_id}/") as response:
            if response.status == 200:
                json_data = await response.json()
                return json_data
            elif response.status == 404:
                print(f"Персонаж с ID {person_id} не найден")
                return None
            else:
                print(f"Ошибка {response.status} для персонажа {person_id}")
                return None
    except Exception as e:
        print(f"Ошибка при запросе персонажа {person_id}: {e}")
        return None


def extract_person_data(json_data):
    """Извлекает нужные поля из ответа API"""
    if not json_data or "result" not in json_data:
        return None
    
    result = json_data["result"]
    properties = result.get("properties", {})
    
    return {
        "id": result.get("uid"),
        "birth_year": properties.get("birth_year"),
        "eye_color": properties.get("eye_color"),
        "gender": properties.get("gender"),
        "hair_color": properties.get("hair_color"),
        "homeworld": properties.get("homeworld"),
        "mass": properties.get("mass"),
        "name": properties.get("name"),
        "skin_color": properties.get("skin_color"),
    }


async def insert_people(people_data_list: list[dict]):
    """Вставляет данные о персонажах в базу данных"""
    if not people_data_list:
        return
    
    async with DbSession() as session:
        for person_data in people_data_list:
            if person_data:
                try:
                    swapi_person = SwapiPeople(
                        id=int(person_data["id"]) if person_data["id"] else None,
                        birth_year=person_data["birth_year"],
                        eye_color=person_data["eye_color"],
                        gender=person_data["gender"],
                        hair_color=person_data["hair_color"],
                        homeworld=person_data["homeworld"],
                        mass=person_data["mass"],
                        name=person_data["name"],
                        skin_color=person_data["skin_color"],
                    )
                    session.add(swapi_person)
                except Exception as e:
                    print(f"Ошибка при добавлении персонажа {person_data.get('id')}: {e}")
        
        try:
            await session.commit()
            print(f"Успешно добавлено {len(people_data_list)} персонажей")
        except Exception as e:
            print(f"Ошибка при коммите транзакции: {e}")
            await session.rollback()


async def main():
    await init_orm()
    
    async with aiohttp.ClientSession() as http_session:
        # Получаем общее количество персонажей
        try:
            async with http_session.get(f"{API_BASE_URL}") as response:
                if response.status == 200:
                    data = await response.json()
                    total_count = int(data.get("total_records", 0))
                    print(f"Всего персонажей в API: {total_count}")
                else:
                    print("Не удалось получить количество персонажей")
                    total_count = 82  # Примерное количество по документации
        except Exception as e:
            print(f"Ошибка при получении общего количества: {e}")
            total_count = 82
        
        people_ids = range(1, total_count + 1)
        
        for chunk_num, people_ids_chunk in enumerate(chunked(people_ids, MAX_REQUESTS), 1):
            print(f"Обрабатываю чанк {chunk_num}: ID {min(people_ids_chunk)}-{max(people_ids_chunk)}")
            
            coros = [get_people(i, http_session) for i in people_ids_chunk]
            results = await asyncio.gather(*coros)
            
            # Извлекаем нужные данные
            people_data = [extract_person_data(r) for r in results]
            # Фильтруем None
            valid_people_data = [p for p in people_data if p]
            
            if valid_people_data:
                await insert_people(valid_people_data)
            else:
                print("Нет валидных данных в чанке")
    
    await close_orm()


if __name__ == "__main__":
    start = datetime.datetime.now()
    print(f"Начало загрузки в {start}")
    
    asyncio.run(main())
    
    end = datetime.datetime.now()
    print(f"Загрузка завершена в {end}")
    print(f"Общее время выполнения: {end - start}")