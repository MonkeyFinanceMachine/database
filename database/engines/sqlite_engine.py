import sqlite3

from .base_engine import BaseEngine, Table


class SQLiteTable(Table):
    def process_db_row(self,
                       row_data: tuple[str],
                       row_key: str,
                       conditions: dict[str, object] | None = None) -> list[dict]:
        data = dict(zip(self.columns, row_data))
        return [data]

    def make_db_row(self,
                    table_name: str,
                    row_data: dict) -> tuple:
        key = f"INSERT OR REPLACE INTO {table_name} VALUES({','.join(['?'] * len(self.columns))})"
        value = list(tuple(row_data.values()))
        return (key, value)


class SQLiteEngine(BaseEngine):
    connection: sqlite3.Connection

    def __init__(self,
                 path: str,
                 timeout: int = 5) -> None:
        super().__init__(path)
        self.connection = sqlite3.connect(path,
                                          timeout=timeout,
                                          check_same_thread=False,
                                          autocommit=True)

    def create_table(self,
                     name: str,
                     columns: list[str],
                     key: str | None = None) -> None:
        self.tables[name] = SQLiteTable(columns, key)
        cur = self.connection.cursor()
        if key is not None:
            columns.append(f"PRIMARY KEY({key})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {name}({",".join(columns)})")
        self.connection.commit()
        cur.close()

    def rename_table(self,
                     old_name: str,
                     new_name: str) -> None:
        cur = self.connection.cursor()
        cur.execute(f"ALTER TABLE `{old_name}` RENAME TO `{new_name}`")
        self.connection.commit()
        cur.close()

    def delete_table(self, name: str) -> None:
        cur = self.connection.cursor()
        cur.execute(f"DROP TABLE `{name}`")
        self.connection.commit()
        cur.close()

    async def select(self,
                     table_name: str,
                     conditions: dict[str, object] | None = None) -> list[dict]:
        table: SQLiteTable = self.tables[table_name]
        cur = self.connection.cursor()
        sql = f"SELECT * FROM {table_name}" + self._sqlite_make_where(conditions)
        cur.execute(sql)

        result = []
        for values in cur.fetchall():
            result += table.process_db_row(values, "")
        cur.close()
        return result

    async def insert(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        table: SQLiteTable = self.tables[table_name]
        cur = self.connection.cursor()
        key, value = table.make_db_row(table_name, row_data)
        cur.execute(key, value)
        self.connection.commit()
        cur.close()

    async def delete(self,
                     table_name: str,
                     row_data: dict[str, object]) -> None:
        cur = self.connection.cursor()
        sql = f"DELETE FROM {table_name}" + self._sqlite_make_where(row_data)
        cur.execute(sql)
        self.connection.commit()
        cur.close()

    def _sqlite_make_where(self, conditions: dict[str, object] | None = None) -> str:
        if conditions is not None:
            where_statements = []
            for key in conditions:
                where_statements.append(f"{key} = '{str(conditions[key])}'")
            return " WHERE " + " AND ".join(where_statements)
        return ""
