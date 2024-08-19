import asyncio
from dataclasses import dataclass
from pprint import pprint
from random import uniform

from database import BaseEntity, Database


class User(BaseEntity):
    name: str  # колонки должны начинаться с буквы и должны быть указаны в аннотациях
    dick_size: int = 666

    def __init__(self, name: str) -> None:
        self.name = name
        self.fuck_c = "YES"  # это не будет колонкой и не загрузится в/из БД!
        super().__init__()  # эта строчка обязательна в конце конструктора

    def __repr__(self) -> str:
        return f"User(name={self.name}, dick_size={self.dick_size})"


@dataclass
class PhotoRequest(BaseEntity):
    photo_id: int  # TODO: автоинкремент ключа
    path: str

    def __post_init__(self) -> None:
        super().__init__()  # если используется dataclass, то обязательно прописать это в конце __post_init__


# инициализация Database обязательно после появления в globals всех entities!
# то бишь либо сначала импортишь их
# либо если в одном файле, то после объявления
db = Database("/tmp/wabada")

users = [User("Bob")] + [User("r" * int(uniform(1, 15))) for i in range(3)]
new_users = []
bob = []
print(f"Table: {users[0].__tablename__}, columns: {users[0].__properties__}")

photo_requests = [PhotoRequest(int(uniform(1, 15)), "r" * int(uniform(1, 15))) for i in range(4)]
new_requests = []
print(f"Table: {photo_requests[0].__tablename__}, columns: {photo_requests[0].__properties__}")


# загружаем в базу
async def load_to_db():
    for user in users:
        await db.push(user)

    for ph_request in photo_requests:
        await db.push(ph_request)


# получаем из базы
async def get_from_db():
    new_users = await db.pull(User)
    bob = await db.pull(User, name="Bob")  # выставляем хрюсловие
    low_penis = await db.pull(User, dick_size=1)

    new_requests = await db.pull(PhotoRequest)
    return new_users, bob, new_requests, low_penis


async def main():
    await load_to_db()
    new_users, bob, new_requests, low_penis = await get_from_db()
    print("What we send:")
    pprint(users)
    pprint(photo_requests)
    print()
    print("What we get:")
    pprint(new_users)
    pprint(new_requests)
    print()
    print("Bob:")
    pprint(bob)
    print()
    print("Low penises:")
    pprint(low_penis)


if __name__ == "__main__":
    asyncio.run(main())
