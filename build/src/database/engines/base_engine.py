from abc import abstractmethod, ABC
import os

from libscrc import iso


def crc64(obj: object) -> int:
    return iso(str(obj).encode())


class Table(ABC):
    columns: dict[str, object]
    key: str = None

    def __init__(self,
                 columns: list[str],
                 key: str | None = None) -> None:
        self.columns = columns
        if key is not None:
            if key not in columns:
                raise ValueError(f"Key {key} not presented in column list!")
            self.key = key

    @abstractmethod
    def process_db_row(self,
                       row_data: object,
                       row_key: str,
                       conditions: dict[str, object] | None = None) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def make_db_row(self,
                    row_data: dict) -> tuple:
        raise NotImplementedError


class BaseEngine(ABC):
    path: str
    tables: dict[str, Table]

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.sep.join(os.path.normpath(path).split(os.sep)[:-1]), exist_ok=True)
        self.tables = dict()

    @abstractmethod
    def create_table(self,
                     name: str,
                     columns: list[str],
                     key: str | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def rename_table(self,
                     old_name: str,
                     new_name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_table(self, name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def select(self,
                     table_name: str,
                     conditions: dict[str, object] | None = None) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    async def insert(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        raise NotImplementedError
