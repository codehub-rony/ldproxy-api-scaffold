import yaml
import os
from typing import List, Optional
from ldproxy_api_scaffold.core.api_service import ApiService
from ldproxy_api_scaffold.core.sql_provider import SQLProvider
from ldproxy_api_scaffold.core.tile_provider import TileProvider
from ldproxy_api_scaffold.utils.db import DatabaseClient

class APIConfig():
    """
    Generates LDProxy-compatible configuration files for service, SQL provider, and tile provider.

    This class serves as the main entry point for generating LDProxy configurations. It connects
    to a PostgreSQL/PostGIS database, analyzes table structures, and produces all necessary
    YAML configuration files required by LDProxy.

    The generated configuration includes:
    - Service configuration (api_service.yml)
    - SQL provider configuration (sql_provider.yml)
    - Tile provider configuration (tile_provider.yml)

    Each configuration file is generated based on:
    - Database schema and table structure
    - Selected API building blocks
    - Docker environment settings
    - Target table selection

    Attributes:
        service_id (str): Identifier used for the LDProxy service.
        schema_name (str): Name of the schema in the database.
        db_conn_str (str): SQLAlchemy-compatible connection string to the database.
        db_host_template_str (str, optional): Template connection string for provider configuration.
            If not provided, uses db_conn_str.
        target_tables (List[str], optional): Specific tables to include in the configuration.
            If None, all tables in the schema are used.
        api_blocks (List[str], optional): List of enabled API features. Default includes:
            - QUERYABLES: Property-based querying
            - CRS: Coordinate reference systems
            - FILTER: Filtering capabilities
            - TILES: Tile-based data access
            - STYLES: Styling capabilities
            - PROJECTIONS: Map projection support
        run_in_docker (bool): Flag indicating whether Docker-compatible paths should be used.
            Affects hostname resolution in provider configurations.
        db_client (DatabaseClient): Client for database operations.
        table_config (dict): Configuration for tables and their columns.
        service_obj (ApiService): Service configuration generator.
        sql_provider_obj (SQLProvider): SQL provider configuration generator.
        tile_provider_obj (TileProvider): Tile provider configuration generator.

    Methods:
        _init_resources():
            Internal method to initialize database connection and provider objects.

        generate(export_dir: str):
            Generates and exports all YAML configuration files.

        dispose_engine():
            Cleans up database engine resources.
    """
    def __init__(self, service_id:str, schema_name:str, db_conn_str:str, db_host_template_str: Optional[str] = None, target_tables:Optional[List[str]] = None,
                        api_blocks:Optional[List[str]] = None,
                        run_in_docker:bool = False):
        """
        Initialize the APIConfig generator.

        Args:
            service_id (str): Unique identifier for the LDProxy service.
            schema_name (str): Name of the database schema to analyze.
            db_conn_str (str): SQLAlchemy connection string for database access.
            db_template_str (str, optional): Template connection string for provider config.
                If not provided, uses db_conn_str. Useful when provider needs different
                connection settings than the analysis phase.
            target_tables (List[str], optional): Specific tables to include in the config.
                If None, all tables in the schema are included.
            api_blocks (List[str], optional): List of API features to enable.
                Defaults to ["QUERYABLES", "CRS", "FILTER", "TILES", "STYLES", "PROJECTIONS"].
            run_in_docker (bool, optional): Whether to use Docker-compatible settings.
                Defaults to False.
        """
        self.service_id = service_id
        self.schema_name = schema_name
        self.db_conn_str = db_conn_str
        self.db_host_template_str = db_host_template_str
        self.target_tables = target_tables
        self.api_blocks = api_blocks or ["QUERYABLES", "CRS", "FILTER", "TILES", "STYLES", "PROJECTIONS"]
        self.run_in_docker = run_in_docker

        self.db_client = DatabaseClient(self.db_conn_str, self.schema_name)

        self._init_resources()

    def _init_resources(self):
        """
        Internal method to initialize resources by connecting to the database,
        fetching table metadata, and initializing provider objects.

        This method:
        1. Determines target tables (all schema tables if not specified)
        2. Creates table configuration with column metadata
        3. Initializes service, SQL provider, and tile provider objects
        4. Sets up all necessary configuration structures

        The initialized objects are used by the generate() method to create
        the final YAML configuration files.
        """
        if self.target_tables is None:
            self.target_tables = self.db_client.get_schema_tables()

        self.table_config = self.db_client.create_table_config(self.target_tables)

        self.service_obj = ApiService(self.service_id, self.table_config, self.api_blocks)
        self.sql_provider_obj = SQLProvider(self.service_id,
                                            table_config=self.table_config,
                                            engine=self.db_client.engine,
                                            db_host_template_str=self.db_host_template_str,
                                            run_in_docker=self.run_in_docker)
        self.tile_provider_obj = TileProvider(self.service_id, self.table_config)

    def generate(self, export_dir:str):
        """
        Generates and exports YAML files for LDProxy.

        This method creates three configuration files in the specified directory:
        - {service_id}.yml: Service configuration
        - {service_id}-tiles.yml: Tile provider configuration
        - {service_id}.yml: SQL provider configuration

        The files are saved in appropriate subdirectories:
        - services/: For service configuration
        - providers/: For provider configurations

        Args:
            export_dir (str): Base directory where the configuration files will be saved.
                The files will be organized in 'services' and 'providers' subdirectories.
        """

        self.service_obj.create_yaml(export_dir)
        self.sql_provider_obj.create_yaml(export_dir)
        self.tile_provider_obj.create_yaml(export_dir)

        self.dispose_engine()

    def dispose_engine(self):
        self.db_client.dispose_engine()
