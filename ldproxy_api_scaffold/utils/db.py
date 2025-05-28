from sqlalchemy import create_engine, inspect, make_url
from sqlalchemy.engine.url import make_url
from typing import List

class DatabaseClient:
    def __init__(self, conn_str:str, schema:str):
        self.conn_str = conn_str
        self.url = make_url(conn_str)
        self.user = self.url.username
        self.password = self.url.password
        self.host = self.url.host
        self.port = self.url.port
        self.database = self.url.database
        self.schema = schema

        self.engine = create_engine(conn_str)
        self.inspector = inspect(self.engine)

    def create_table_config(self, tablenames:List[str]):
        table_config = {"db_schema": self.schema, "tables": []}

        for tablename in tablenames:

            columns = self.inspector.get_columns(tablename, schema=self.schema)
            table_config["tables"].append({"tablename": tablename, "columns":columns })

        return table_config

    def get_schema_tables(self):
        return self.inspector.get_table_names(self.schema)

    def dispose_engine(self):
        self.engine.dispose()