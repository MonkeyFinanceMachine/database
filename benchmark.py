import asyncio
from dataclasses import dataclass
import logging
import requests
import time

from database import Database, BaseEntity


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
N_TIMES = 10


@dataclass
class Character(BaseEntity):
    name: str
    height: int

    def __post_init__(self) -> None:
        super().__init__()


lmdb = Database("/tmp/benchmark/lmdb")
sqlite = Database("/tmp/benchmark/sqlite", engine='sqlite')


def timer(func):
    def timed_func(**args):
        start = time.time()
        for i in range(N_TIMES):
            func(**args)
        end = time.time()
        total = round(1000 * (end - start) / N_TIMES, 3)
        logger.info(f'Mean time for {N_TIMES} runs taken in seconds for {func.__name__} - {total}ms')
    return timed_func


async def get_people(db: Database, number: int) -> None:
    url = f"https://swapi.dev/api/people/{number}/"
    response = requests.get(url)
    data = response.json()
    character = Character(data['name'], data['height'])
    await database_manage(db, character)


async def database_manage(db: Database, character: Character) -> None:
    await db.push(character)


async def asyncio_wrapper(db: Database, input_value: int):
    tasks = [get_people(db, i) for i in input_value]
    asyncio.gather(*tasks)


@timer
def using_lmdb(input_value):
    asyncio.run(asyncio_wrapper(db=lmdb, input_value=input_value))


@timer
def using_sqlite(input_value):
    asyncio.run(asyncio_wrapper(db=sqlite, input_value=input_value))


if __name__ == '__main__':
    input_value = list(range(1, 10))
    using_lmdb(input_value=input_value)
    using_sqlite(input_value=input_value)
