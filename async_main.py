import asyncio
from pprint import pprint
import aiohttp
import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Integer, String, Column
from dotenv import load_dotenv
import os


# Реализуем подключение к базе данных
load_dotenv()
DB = os.getenv("PG_DB")
PG_DSN = f'postgresql+asyncpg://user:1234@127.0.0.1:5431/{DB}'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_= AsyncSession,
                       expire_on_commit=False)
# expire_on_commit=False чтобы сессия не истекала после коммита
Base = declarative_base()

# Описаываем модель
class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)



async def download_links(links_list, client_session):
    '''
    Функция загрузки данных по линкам на объекты

    :param links_list: список линков для загрузки данных
    :param client_session: клиентская сессия в рамках которой производиться
    загрузка
    :return: список с выгруженными данными
    '''
    coros = []
    for link in links_list:
        coro = client_session.get(link)
        coros.append(coro)
    http_responces = await asyncio.gather(*coros)

    json_coros = []
    for http_responce in http_responces:
        json_coro = http_responce.json()
        json_coros.append(json_coro)

    return await asyncio.gather(*json_coros)

async def create_str(obj_list: list, obj_name: str):
    str_for_insert_db = ''
    if len(obj_list) != 0:
        for object in obj_list:
            if obj_list.index(object) + 1 != len(obj_list):
                str_for_insert_db += object[obj_name] + ','
            else:
                str_for_insert_db += object[obj_name]
    return str_for_insert_db

async def paste_to_db(people_list):
    '''
    Функция вставки в БД скачанных данных по персонажу

    :param people_list: список с данными по персонажам
    :return:
    '''
    async with Session() as session:
        # orm_objects = [SwapiPeople(json=item) for item in people_list]
        # session.add_all(orm_objects)
        for item in people_list:
            # Объявляем корутины для скачивания информации по полученным ранее
            # линкам
            film_str_coro = create_str(item['films'], 'title')
            species_str_coro = create_str(item['species'], 'name')
            starships_str_coro = create_str(item['starships'], 'model')
            vehicles_str_coro = create_str(item['starships'], 'name')

            # Выкачиваем асинхронно инфо по ранее подготовленным корутинам
            all_strings = await asyncio.gather(film_str_coro,
                                               species_str_coro,
                                               starships_str_coro,
                                               vehicles_str_coro)

            # Распаковываем полученный список
            film_str, species_str, starships_str, vehicles_str = all_strings

            # Вставляем объект в базу
            hero = SwapiPeople(
                name=item['name'],
                birth_year=item['birth_year'],
                eye_color=item['eye_color'],
                films=film_str,
                gender=item['gender'],
                hair_color=item['hair_color'],
                height=item['height'],
                homeworld=item['homeworld'],
                mass=item['mass'],
                skin_color=item['skin_color'],
                species=species_str,
                starships=starships_str,
                vehicles=vehicles_str
            )
            session.add(hero)

        await session.commit()
    return


async def get_people(people_id, client_session):
    '''
    Функция получения данных по персонажу от сервиса swapi.dev по его ID

    :param people_id: ID персонажа
    :param client_session: открытая сессия
    :return: словарь с данными по персонажу
    '''
    async with client_session.get(f'https://swapi.dev/api/people/{people_id}'
                                  ) as response:
        json_data = await response.json()

        # Получаем списки линков на объекты согласно ДЗ: фильмы, типы,
        # корабли, транспорт
        film_links = json_data.get('films', [])
        species_links = json_data.get('species', [])
        starships_links = json_data.get('starships', [])
        vehicles_links = json_data.get('vehicles', [])
        homeworld_link = json_data.get('homeworld', [])

        # Объявляем корутины для скачивания информации по полученным ранее
        # линкам
        films_coro = download_links(film_links, client_session)
        species_coro = download_links(species_links, client_session)
        starships_coro = download_links(starships_links, client_session)
        vehicles_coro = download_links(vehicles_links, client_session)

        # Выкачиваем асинхронно инфо по ранее подготовленным корутинам
        fields = await asyncio.gather(films_coro,
                                      species_coro,
                                      starships_coro,
                                      vehicles_coro)

        # Распаковываем полученный список
        films, species, starships, vehicles = fields

        # Заливаем в исходный словарь с данными по персонажу название объектов
        # вместо линков
        json_data['films'] = films
        json_data['species'] = species
        json_data['starships'] = starships
        json_data['vehicles'] = vehicles

        # Печать полученных данных по персонажу для проверки
        print('-- Start printing ', json_data['name'], ' data ', '-'*20)
        pprint(json_data)
        print('-- Finised printing ', json_data['name'], ' data ', '-'*20)
        print()

        return json_data


async def main():
    # Проводим миграцию в БД
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    # Выкачиваем первые Х записей
    async with aiohttp.ClientSession() as client_session:
        coros = []
        for i in range(1, 11):
            coro = get_people(i, client_session)
            coros.append(coro)
        results = await asyncio.gather(*coros)

    # Создаем задачу для вставки results в базу, которая начинает выполняться
    # в момент создания и не блокирует выполнение дальнейшего кода
    paste_to_db_task = asyncio.create_task(paste_to_db(people_list=results))

    # Чтобы гарантированно закончить вставку в базу необходимо закрыть все
    # открытые ранее задачи.
    # Если задача одна, как у нас
    await paste_to_db_task

    # Если задач много
    # all_tasks = asyncio.all_tasks()
    # all_tasks = all_tasks - {asyncio.current_task()}
    # await asyncio.gather(*all_tasks)


# Засекаем время старта для вычисления общего времени работы
start_time = datetime.datetime.now()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

# Рассчитываем и выводим общее время работы
print('Elapsed time: ', datetime.datetime.now() - start_time)
