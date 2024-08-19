from .engines import LMDBEngine, SQLiteEngine
from .entities import BaseEntity


class Database:
    def __init__(self,
                 path: str,
                 engine: str = 'lmdb',
                 **engine_kwargs) -> None:
        if engine == 'lmdb':
            self.engine = LMDBEngine(path, **engine_kwargs)
        elif engine == 'sqlite':
            self.engine = SQLiteEngine(path, **engine_kwargs)
        else:
            raise ValueError(f"Unknown engine '{engine}' passed!")
        self._init_db()

    async def push(self, entity: BaseEntity) -> None:
        data = entity._serialize()
        await self.engine.insert(entity.__tablename__, data)

    async def pull(self, entity_cls: BaseEntity, **conditions) -> list[BaseEntity]:
        entity = self._revoke_entity(entity_cls)
        rows = await self.engine.select(entity.__tablename__,
                                        conditions if conditions else None)
        result = []
        for data in rows:
            entity = self._revoke_entity(entity_cls)
            result.append(entity._unserialize(data))
        return result

    async def drop(self, entity: BaseEntity) -> None:
        data = entity._serialize()
        await self.engine.delete(entity.__tablename__, data)

    def _init_db(self) -> None:
        for entity_cls in self._get_all_entities():
            entity = self._revoke_entity(entity_cls)
            self.engine.create_table(entity.__tablename__,
                                     entity._get_props())

    def _get_all_entities(self, cls=BaseEntity) -> list[BaseEntity]:
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in self._get_all_entities(cls=c)]
        )

    def _revoke_entity(self, entity_cls) -> BaseEntity:
        entity: BaseEntity = entity_cls.__new__(entity_cls)
        super(type(entity), entity).__init__()
        return entity
