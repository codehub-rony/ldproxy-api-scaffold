from sqlalchemy import create_engine, inspect
from typing import List

def connect_to_db(conn_str:str):
    engine = create_engine(conn_str)
    return engine, inspect(engine)

def create_table_config(tablenames:List[str], db_schema:str, insp):
    table_config = {"db_schema": db_schema, "tables": []}

    for tablename in tablenames:

        columns = insp.get_columns(tablename, schema=db_schema)
        table_config["tables"].append({"tablename": tablename, "columns":columns })

    return table_config
