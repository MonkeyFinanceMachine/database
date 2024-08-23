from copy import deepcopy
from multiprocessing import cpu_count
from pickle import dumps, loads
import shutil

import lmdb

from .base_engine import BaseEngine, Table, crc64


# Сохраняет все данные через pickle
class LMDBTable(Table):
    def process_db_row(self,
                       row_data: bytes,
                       row_key: str,
                       conditions: dict[str, object] | None = None) -> list[dict]:
        data = loads(row_data)
        if self.key is not None:
            data[self.key] = row_key.decode()

        if conditions is not None:
            for key in conditions:
                if key not in self.columns:
                    raise KeyError(f"Key {key} not presented in column list!")
                if data[key] != str(conditions[key]):
                    return []
        return [data]

    def make_db_row(self,
                    row_data: dict) -> tuple:
        if self.key is not None:
            key = str(row_data[self.key]).encode()
            del row_data[key]
        else:
            key = crc64(row_data).to_bytes(8)

        value = dumps(row_data)
        return (key, value)


class LMDBEngine(BaseEngine):
    environment: lmdb.Environment
    db_descriptors: dict[str, lmdb._Database]

    def __init__(self,
                 path: str,
                 threads_count: int = -1,
                 map_size: int = 2**32) -> None:
        super().__init__(path)

        if threads_count == -1:
            self.threads_count = cpu_count()
        elif threads_count <= 0:
            raise ValueError('"threads_count" must be greater than zero or -1 to use all CPU cores!')
        else:
            self.threads_count = threads_count
        self.map_size = map_size

        self.environment = self._lmdb_open(self.path)
        self.db_descriptors = dict()

    def create_table(self,
                     name: str,
                     columns: list[str],
                     key: str | None = None) -> None:
        self.tables[name] = LMDBTable(columns, key)
        self._lmdb_get_db_descriptor(self.environment,
                                     self.tables[name],
                                     name)

    # TODO: сейчас экстенсивно копируем все таблицы, переоткрывая нужную, надо побыстрее
    def rename_table(self,
                     old_name: str,
                     new_name: str) -> None:
        tmp_env = self._lmdb_create_tmp()

        for table_name in self.tables:
            table_db = self.db_descriptors[table_name]
            if table_name == old_name:
                table_name = new_name
            tmp_db = tmp_env.open_db(table_name.encode(),
                                     integerkey=self.tables[table_name].key is None)
            with self.environment.begin(db=table_db) as table_txn:
                with tmp_env.begin(db=tmp_db) as tmp_txn:
                    for key, value in table_txn.cursor():
                        tmp_txn.put(key, value)
        tmp_env.close()
        self.environment.close()

        self._lmdb_copy_tmp()

        self.tables[new_name] = deepcopy(self.tables[old_name])
        del self.tables[old_name]
        self._lmdb_sync()

    # TODO: сейчас экстенсивно копируем все таблицы, кроме удаляемой, надо побыстрее
    def delete_table(self, name: str) -> None:
        tmp_env = self._lmdb_create_tmp()

        for table_name in self.tables:
            if table_name == name:
                continue
            table_db = self.db_descriptors[table_name]
            tmp_db = tmp_env.open_db(table_name.encode(),
                                     integerkey=self.tables[table_name].key is None)
            with self.environment.begin(db=table_db) as table_txn:
                with tmp_env.begin(db=tmp_db) as tmp_txn:
                    for key, value in table_txn.cursor():
                        tmp_txn.put(key, value)
        tmp_env.close()
        self.environment.close()

        self._lmdb_copy_tmp()

        del self.tables[name]
        self._lmdb_sync()

    async def select(self,
                     table_name: str,
                     conditions: dict[str, object] | None = None) -> list[dict]:
        result = []
        table: LMDBTable = self.tables[table_name]
        if table.key is not None and table.key in conditions:
            with self.environment.begin(db=self.db_descriptors[table_name]) as txn:
                value = txn.get(str(conditions[table.key]).encode())
                if value is not None:
                    result += table.process_db_row(value, conditions[table.key], conditions)
        else:
            with self.environment.begin(db=self.db_descriptors[table_name]) as txn:
                for key, value in txn.cursor():
                    row = table.process_db_row(value, key, conditions)
                    result += row
        return result

    async def insert(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        table: LMDBTable = self.tables[table_name]
        with self.environment.begin(write=True, db=self.db_descriptors[table_name]) as txn:
            key, value = table.make_db_row(row_data)
            txn.put(key, value)

    async def delete(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        table: LMDBTable = self.tables[table_name]
        if table.key is not None and table.key in row_data:
            key = str(row_data[table.key]).encode()
            with self.environment.begin(write=True, db=self.db_descriptors[table_name]) as txn:
                txn.delete(key)
        else:
            with self.environment.begin(write=True, db=self.db_descriptors[table_name]) as txn:
                keys_for_delete = []
                for key, value in txn.cursor():
                    row = table.process_db_row(value, key, conditions=row_data)
                    if row:
                        keys_for_delete.append(key)
                for key in keys_for_delete:
                    txn.delete(key)

    def _lmdb_open(self, path: str) -> lmdb.Environment:
        return lmdb.open(path,
                         map_size=self.map_size,
                         subdir=True,
                         metasync=False,
                         sync=False,
                         map_async=True,
                         max_readers=2**16,
                         max_dbs=2**16,
                         max_spare_txns=self.threads_count)

    def _lmdb_get_db_descriptor(self,
                                env: lmdb.Environment,
                                table: LMDBTable,
                                table_name: str) -> None:
        self.db_descriptors[table_name] = env.open_db(table_name.encode(),
                                                      integerkey=table.key is None)

    def _lmdb_sync(self) -> None:
        self.environment = self._lmdb_open(self.path)
        for table_name in self.tables:
            self._lmdb_get_db_descriptor(self.environment,
                                         self.tables[table_name],
                                         table_name)

    def _lmdb_create_tmp(self) -> lmdb.Environment:
        shutil.rmtree("/tmp/lmdb-copy", ignore_errors=True)
        return self._lmdb_open("/tmp/lmdb-copy")

    def _lmdb_copy_tmp(self) -> lmdb.Environment:
        shutil.rmtree(self.path)
        shutil.copytree("/tmp/lmdb-copy", self.path)
        shutil.rmtree("/tmp/lmdb-copy")
