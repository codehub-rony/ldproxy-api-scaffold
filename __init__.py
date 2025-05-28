from typing import List, Optional
from sqlalchemy.engine import Engine
from .config.service import Service
from .config.sql_provider import SQLProvider
from .config.tile_provider import TileProvider

def create_config(
    service_id: str,
    schema: str,
    engine: Engine,
    tables: Optional[List[str]] = None,
    api_building_blocks: Optional[List[str]] = None,
    force_axis_order: str = "LON_LAT",
    docker: bool = False,
    output_dir: str = "export"
) -> None:
    """
    Create ldproxy configuration files for a given database schema.
    
    Args:
        service_id (str): Unique identifier for the service
        schema (str): Database schema name
        engine (Engine): SQLAlchemy engine instance
        tables (List[str], optional): List of tables to include. If None, includes all tables
        api_building_blocks (List[str], optional): List of API building blocks to include.
            Available options: ['QUERYABLES', 'TILES', 'CRS', 'STYLES', 'FILTER', 'PROJECTIONS']
            If None, includes all building blocks
        force_axis_order (str): Coordinate order for geometries. Defaults to "LON_LAT"
        docker (bool): Whether running in Docker environment. Defaults to False
        output_dir (str): Directory to output configuration files. Defaults to "export"
    """
    # Default building blocks if none specified
    if api_building_blocks is None:
        api_building_blocks = ['QUERYABLES', 'TILES', 'CRS', 'STYLES', 'FILTER', 'PROJECTIONS']
    
    # Get table information
    inspector = inspect(engine)
    if tables is None:
        tables = inspector.get_table_names(schema=schema)
    
    # Create table configuration
    table_config = {
        "db_schema": schema,
        "tables": []
    }
    
    for table_name in tables:
        columns = inspector.get_columns(table_name, schema=schema)
        table_config["tables"].append({
            "tablename": table_name,
            "columns": columns
        })
    
    # Create database configuration
    db_config = {
        "DB_SCHEMA": schema,
        "DATABASE": engine.url.database,
        "DB_HOST": engine.url.host,
        "DB_PORT": engine.url.port,
        "DB_USER": engine.url.username,
        "DB_PASSWORD": engine.url.password
    }
    
    # Create and export configurations
    service = Service(service_id, table_config, api_building_blocks, engine)
    sql_provider = SQLProvider(service_id, force_axis_order, table_config, engine, db_config, docker)
    tile_provider = TileProvider(service_id, table_config)
    
    # Create output directories
    os.makedirs(os.path.join(output_dir, "services"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "providers"), exist_ok=True)
    
    # Export configurations
    service.create_yaml(output_dir=output_dir)
    sql_provider.create_yaml(output_dir=output_dir)
    tile_provider.create_yaml(output_dir=output_dir) 