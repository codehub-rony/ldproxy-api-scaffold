import time
from sqlalchemy import VARCHAR, Text, String, TIMESTAMP, text, Integer, BIGINT, DOUBLE_PRECISION
import os
import base64
import yaml
from typing import Optional

class SQLProvider:
    """
    A class to generate ldproxy provider configuration for SQL-based feature services.

    This class handles the creation of provider configurations for SQL databases,
    particularly focusing on PostGIS-enabled databases. It manages type mappings,
    geometry handling, and database connection settings.

    Attributes:
        service_id (str): Identifier of the ldproxy service.
        engine: SQLAlchemy engine used to connect to the database.
        db_host_template_str (str): Template string for database host configuration.
        run_in_docker (bool): Whether the service runs in Docker environment.
        db_config (dict): Dictionary with database connection information.
        table_config (dict): Table and column configuration for the provider.
        config (dict): Internal configuration object representing the provider YAML structure.

    Methods:
        map_datatype(data_type):
            Converts SQLAlchemy data types to ldproxy-compatible type strings.

        map_geom_type(geom_type):
            Maps raw geometry types to ldproxy-compatible strings.

        create_types():
            Populates the configuration with type definitions for each table.

        get_geometry_type(tablename):
            Retrieves and maps the geometry type of a table from the PostGIS metadata.

        create_table_properties(columns, tablename):
            Creates a dictionary of ldproxy property definitions for table columns.

        create_connection_info_dict():
            Constructs the dictionary with database connection info.

        create_yaml(export_dir):
            Exports the full configuration as a YAML file.
    """
    def __init__(self, service_id:str, table_config:dict, engine, db_host_template_str:Optional[str], force_axis_order:Optional[bool]=True, run_in_docker:Optional[bool]=False):
        """
        Initialize a new SQLProvider instance.

        Args:
            service_id (str): Unique identifier for the ldproxy service.
            table_config (dict): Configuration with schema, tables, and column metadata.
            engine: SQLAlchemy engine for connecting to the database.
            db_conn_str (str): Database connection string.
            force_axis_order (bool, optional): Whether to enforce axis order for CRS.
                Defaults to True.
            run_in_docker (bool, optional): Whether ldproxy runs in Docker and needs
                special hostname resolution. Defaults to False.
        """
        self.service_id = service_id
        self.engine = engine
        self.db_host_template_str = db_host_template_str
        self.run_in_docker = run_in_docker
        self.db_config = None
        self.table_config = table_config

        native_crs = {
           "code": 4326,
        }

        if force_axis_order:
            native_crs["forceAxisOrder"] = "LON_LAT"

        self.config = {
            "id": service_id,
            "entityStorageVersion": 2,
            "createdAt": round(time.time()),
            "lastModified": round(time.time()),
            "providerType": "FEATURE",
            "providerSubType": "SQL",
            "nativeCrs": native_crs,
            "typeValidation": "NONE",
            "connectionInfo": self.create_connection_info_dict(),
            "sourcePathDefaults": {
                "primaryKey": "id",
                "sortKey": "id"
            },
            "queryGeneration": {
                "chunkSize": 10000,
                "computeNumberMatched": True
            },
            "types": {}
        }

        self.create_types()

    def map_datatype(self, data_type):
        """
        Maps SQLAlchemy column types to ldproxy-compatible type strings.

        This method handles the conversion of SQLAlchemy data types to the format
        expected by ldproxy. It supports common types like strings, timestamps,
        and integers.

        Args:
            data_type: A SQLAlchemy type object (e.g., VARCHAR, TIMESTAMP, Integer).

        Returns:
            str: A string representing the mapped type for ldproxy:
                - 'STRING' for VARCHAR, Text, String
                - 'DATETIME' for TIMESTAMP
                - 'INTEGER' for BIGINT, Integer
                - 'FLOAT' for DOUBLE_PRECISION
                - Original type string for other types
        """
        if isinstance(data_type, (VARCHAR, Text, String)):
            return 'STRING'
        elif isinstance(data_type, TIMESTAMP):
            return 'DATETIME'
        elif isinstance(data_type, (BIGINT, Integer)):
            return 'INTEGER'
        elif isinstance(data_type, DOUBLE_PRECISION):
            return 'FLOAT'
        else:
            return f"{data_type}"

    def map_geom_type(self, geom_type):
        """
        Converts raw geometry type names to ldproxy-compatible geometry types.

        This method handles the mapping between PostGIS geometry types and the
        format expected by ldproxy. It supports common geometry types like
        points, lines, and polygons.

        Args:
            geom_type (str): Geometry type as stored in `geometry_columns`
                (e.g., 'MULTILINESTRING', 'LINESTRING', 'MULTIPOLYGON').

        Returns:
            str: ldproxy-compatible geometry type string:
                - 'MULTI_LINE_STRING' for MULTILINESTRING
                - 'LINE_STRING' for LINESTRING
                - 'MULTI_POLYGON' for MULTIPOLYGON
                - 'MULTI_POINT' for MULTIPOINT
                - Original type for other geometries
        """
        if geom_type == "MULTILINESTRING":
            return "MULTI_LINE_STRING"
        if geom_type == "LINESTRING":
            return "LINE_STRING"
        if geom_type == "MULTIPOLYGON":
            return "MULTI_POLYGON"
        if geom_type == "MULTIPOINT":
            return "MULTI_POINT"
        else:
            return geom_type

    def create_types(self):
        """
        Builds the 'types' section of the provider configuration.

        This method processes all defined tables and their columns to create
        ldproxy-compatible type definitions. Each table becomes a type with
        its properties mapped from the column definitions.

        The method:
        1. Iterates through all tables in the configuration
        2. Creates property definitions for each column
        3. Adds geometry type information where applicable
        4. Assigns special roles to ID and datetime columns
        5. Updates the internal config object with the type definitions
        """
        for tablename in self.table_config['tables']:
            properties = self.create_table_properties(tablename['columns'], tablename['tablename'])
            self.config["types"][tablename['tablename']] = {"sourcePath": f"/{tablename['tablename']}", "properties": properties}

    def get_geometry_type(self, tablename:str):
        """
        Queries the PostGIS metadata to retrieve the geometry type of a table.

        This method queries the PostGIS geometry_columns metadata table to
        determine the geometry type of a specific table. It handles both
        specific geometry types and generic 'GEOMETRY' types.

        Args:
            tablename (str): Name of the table for which to retrieve geometry type.

        Returns:
            str: Mapped geometry type string or "ANY" if not found or if the type
                is 'GEOMETRY' (generic geometry type).
        """
        geometry_type = "ANY"

        with self.engine.connect() as connection:
            query = text("SELECT type FROM geometry_columns WHERE f_table_name = :table_name AND f_table_schema = :schema_name")
            result = connection.execute(query, {"table_name": tablename, "schema_name": self.table_config['db_schema']})
            geom_type = result.fetchone()

            if geom_type and geom_type[0] != 'GEOMETRY':
                geometry_type = self.map_geom_type(geom_type[0])

        return geometry_type

    def create_table_properties(self, columns:list, tablename:str):
        """
        Creates a dictionary of property definitions for ldproxy.

        This method processes each column in the table to create appropriate
        property definitions, handling special cases for geometry, ID, and
        datetime columns. It ensures proper type mapping and role assignment
        for each column.

        Args:
            columns (list): List of dicts, each with a 'name' and 'type' for a column.
            tablename (str): Name of the table to which the columns belong.

        Returns:
            dict: Mapping of column names to property definitions for ldproxy.
                Special roles are assigned to:
                - 'geom' column: PRIMARY_GEOMETRY
                - 'id' column: ID (with RECEIVABLE excluded)
                - First datetime column: PRIMARY_INSTANT
        """
        properties = {}
        has_datetime_role = False

        for column in columns:
            column_definition = {'sourcePath': f"{column['name']}", "type": self.map_datatype(column['type'])}

            if column['name'] == 'geom':
                column_definition['type'] = "GEOMETRY"
                column_definition['role'] = "PRIMARY_GEOMETRY"
                column_definition['geometryType'] = self.get_geometry_type(tablename)

            elif column['name'] == 'id':
                column_definition['role'] = "ID"
                column_definition['excludedScopes'] = ["RECEIVABLE"]

            elif column_definition['type'] == 'DATETIME' and not has_datetime_role:
                column_definition['role'] = 'PRIMARY_INSTANT'
                has_datetime_role = True

            properties[column['name']] = column_definition

        return properties

    def create_connection_info_dict(self):
        """
        Builds the database connection info dictionary for ldproxy.

        This method creates the connection configuration, handling special cases
        for Docker environments where hostname resolution needs to be adjusted.
        It includes all necessary connection parameters for the database.

        Returns:
            dict: Connection settings including:
                - dialect: Always 'PGIS' for PostGIS
                - database: Database name
                - host: Hostname (adjusted for Docker if needed)
                - user: Database username
                - password: Base64 encoded password
                - schemas: Database schema name
        """
        connection = {}
        connection['dialect'] = 'PGIS'
        connection['database'] = self.engine.url.database
        print(self.run_in_docker)
        if self.run_in_docker:
            connection['host'] = 'host.docker.internal'
        elif self.db_host_template_str:
            connection['host'] = self.db_host_template_str
        else:
            connection['host'] =f"{self.engine.url.host}:{self.engine.url.port}"
        connection['user'] = self.engine.url.username
        connection['password'] = base64.b64encode(self.engine.url.password.encode()).decode()
        connection['schemas'] = self.table_config['db_schema']

        return connection

    def create_yaml(self, export_dir:str):
        """
        Exports the full configuration as a YAML file.

        This method writes the complete provider configuration to a YAML file
        in the specified export directory. The file is saved in the 'providers'
        subdirectory with the service ID as the filename.

        Args:
            export_dir (str): Base directory where the configuration file will be saved.
                The file will be saved in a 'providers' subdirectory.
        """
        export_path = os.path.join(export_dir, 'providers')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.service_id}.yml")
        print('file location:', yaml_file)

        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)



