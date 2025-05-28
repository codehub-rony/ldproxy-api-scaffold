from sqlalchemy import create_engine, inspect, make_url
from sqlalchemy.engine.url import make_url
from typing import List

class DatabaseClient:
    """
    Utility class for connecting to a PostgreSQL/PostGIS database and extracting
    schema and table metadata for LDProxy configuration generation.

    Attributes:
        conn_str (str): Full SQLAlchemy database connection string.
        schema (str): Target schema to operate on.
        url (URL): Parsed connection URL.
        user (str): Database username.
        password (str): Database password.
        host (str): Host of the database server.
        port (int): Port number of the database server.
        database (str): Name of the target database.
        engine (Engine): SQLAlchemy engine object.
        inspector (Inspector): SQLAlchemy inspector for database metadata access.
    """
    def __init__(self, conn_str:str, schema:str):
        """
        Initializes the database client, parses connection string, and prepares
        the SQLAlchemy engine and inspector.

        Args:
            conn_str (str): SQLAlchemy-compatible connection string.
            schema (str): Name of the database schema to inspect.
        """
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
        """
        Builds a configuration dictionary for a list of tables in the schema.

        Args:
            tablenames (List[str]): List of table names to include in the config.

        Returns:
            dict: A dictionary with schema name and column details for each table.
        """
        table_config = {"db_schema": self.schema, "tables": []}

        for tablename in tablenames:

            columns = self.inspector.get_columns(tablename, schema=self.schema)
            table_config["tables"].append({"tablename": tablename, "columns":columns })

        return table_config

    def get_schema_tables(self):
        return self.inspector.get_table_names(self.schema)

    def dispose_engine(self):
        self.engine.dispose()