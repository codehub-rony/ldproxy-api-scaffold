import yaml
import os
from typing import List, Optional
from sqlalchemy import create_engine, inspect
from generators.api_service import ApiService
from generators.sql_provider import SQLProvider
from generators.tile_provider import TileProvider
import utils.db as db

# def connect_to_db(connection_string):
#     """
#     Connects to the database using SQLAlchemy and returns the engine and inspector.
#     Raises an exception if connection fails.
#     """
#     engine = create_engine(connection_string)
#     insp = inspect(engine)
#     return engine, insp

# def get_tables_config(schema_name:str, table_names:List[str], inspector):
#     """
#     Given a schema and a list of tables, returns a dictionary with table info
#     including columns for each table.
#     """
#     config = {"db_schema": schema_name, "tables": []}
#     for table in table_names:
#         columns = inspector.get_columns(table, schema=schema_name)
#         config["tables"].append({"tablename": table, "columns": columns})
#     return config

def generate_api_config(service_id:str, schema_name:str,
                        connection_string:str,
                        target_tables:Optional[List[str]] = None,
                        api_blocks:Optional[List[str]] = None,
                        docker:bool = False) -> dict:
    """
    Main function to generate config files programmatically.

    - Connects to DB
    - Gets table info for specified schema (all tables if target_tables is None)
    - Uses all api_blocks if none provided
    - Creates and exports YAML files using Service, SQLProvider, TileProvider classes

    Returns paths to generated YAML files.
    """
    engine, insp = db.connect_to_db(connection_string)

    # Get all tables if none specified
    if target_tables is None:
        target_tables = insp.get_table_names(schema=schema_name)

    # Default API building blocks
    all_api_blocks = ["QUERYABLES", "CRS", "FILTER", "TILES", "STYLES", "PROJECTIONS"]
    if api_blocks is None:
        api_blocks = all_api_blocks

    table_config = db.create_table_config(schema_name, target_tables, insp)

    service_obj = ApiService(service_id, table_config, api_blocks, engine)
    sql_provider_obj = SQLProvider(service_id, force_axis_order="LON_LAT", table_config=table_config,
                                  engine=engine, db_config=None, docker=docker)
    tile_provider_obj = TileProvider(service_id, table_config)

    service_obj.create_yaml()
    sql_provider_obj.create_yaml()
    tile_provider_obj.create_yaml()

    # Compose output paths (assuming your create_yaml uses export/services/)
    export_dir = os.path.join(os.getcwd(), "export", "services")
    service_file = os.path.join(export_dir, f"{service_id}.yml")
    sql_provider_file = os.path.join(export_dir, f"{service_id}_sqlprovider.yml")  # adjust if needed
    tile_provider_file = os.path.join(export_dir, f"{service_id}_tileprovider.yml")  # adjust if needed

    engine.dispose()

    return {
        "service_yaml": service_file,
        "sql_provider_yaml": sql_provider_file,
        "tile_provider_yaml": tile_provider_file,
    }