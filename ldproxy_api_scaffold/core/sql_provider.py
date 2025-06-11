import time
from sqlalchemy import VARCHAR, Text, String, TIMESTAMP, text, Integer, BIGINT
import os
import base64
import yaml
from typing import Optional

class SQLProvider:
    """
    A class to generate ldproxy provider configuration for SQL-based feature services.

    Attributes:
        service_id (str): Identifier of the ldproxy service.
        docker (bool): True if ldproxy is running inside Docker and needs special hostname resolution.
        engine: SQLAlchemy engine used to connect to the database.
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
    def __init__(self, service_id:str, table_config:dict, engine, db_conn_str, force_axis_order:Optional[bool]=True, run_in_docker:Optional[bool]=False):
        """
        Initializes a new SQLProvider instance.
        Args:
            service_id (str): Unique identifier for the ldproxy service.
            force_axis_order (str): Axis order for CRS (e.g., 'true' to enforce lat/lon order).
            table_config (dict): Configuration with schema, tables, and column metadata.
            engine: SQLAlchemy engine for connecting to the database.
            db_config (dict): Dictionary with database access details.
            docker (bool): True if ldproxy runs in Docker and accesses a database outside the container.
        """
        self.service_id = service_id
        self.engine = engine
        self.db_conn_str = db_conn_str
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
            "connectionInfo": self.create_connection_info_dict(run_in_docker),
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

        Args:
            data_type: A SQLAlchemy type object.

        Returns:
            str: A string representing the mapped type for ldproxy ('STRING', 'INTEGER', etc.).
        """
        if isinstance(data_type, (VARCHAR, Text, String)):
            return 'STRING'

        elif isinstance(data_type, TIMESTAMP):
            return 'DATETIME'
        elif isinstance(data_type, (BIGINT, Integer)):
            return 'INTEGER'
        else:
            return f"{data_type}"

    def map_geom_type(self, geom_type):
        """
        Converts raw geometry type names to ldproxy-compatible geometry types.

        Args:
            geom_type (str): Geometry type as stored in `geometry_columns` (e.g., 'MULTILINESTRING').

        Returns:
            str: ldproxy-compatible geometry type string.
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

        This includes reading all defined tables and their columns and setting
        up ldproxy-compatible property definitions for each.
        """
        for tablename in self.table_config['tables']:
            properties = self.create_table_properties(tablename['columns'], tablename['tablename'])
            self.config["types"][tablename['tablename']] = {"sourcePath": f"/{tablename['tablename']}", "properties": properties}

    def get_geometry_type(self, tablename:str):
        """
        Queries the PostGIS metadata to retrieve the geometry type of a table.

        Args:
            tablename (str): Name of the table for which to retrieve geometry type.

        Returns:
            str: Mapped geometry type string or "ANY" if not found.
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

        Args:
            columns (list): List of dicts, each with a 'name' and 'type' for a column.
            tablename (str): Name of the table to which the columns belong.

        Returns:
            dict: Mapping of column names to property definitions for ldproxy.
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

    def create_connection_info_dict(self, run_in_docker):
        """
        Builds the database connection info dictionary for ldproxy.

        Returns:
            dict: Connection settings including dialect, credentials, host, and schema.
        """
        connection = {}
        connection['dialect'] = 'PGIS'
        connection['database'] = self.engine.url.database
        connection['host'] = 'host.docker.internal' if run_in_docker else self.db_conn_str
        connection['user'] = self.engine.url.username
        connection['password'] = base64.b64encode(self.engine.url.password.encode()).decode()
        connection['schemas'] = self.table_config['db_schema']

        return connection

    def create_yaml(self, export_dir:str):
        """
        Writes the provider configuration as a YAML file to the specified directory.

        Args:
            export_dir (str): Base directory where the 'providers' folder and YAML will be saved.
        """
        export_path = os.path.join(os.getcwd(), export_dir, 'providers')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.service_id}.yml")
        print('file locaiton', yaml_file)

        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)



