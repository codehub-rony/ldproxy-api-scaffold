import time
from sqlalchemy import VARCHAR, Text, String, TIMESTAMP, text, Integer, BIGINT
import os
import base64
import yaml

class SQLProvider:
    """
    A class to represent a provider for generating provider configuration file for ldproxy.

    Attributes:
        service_id (str): The identifier for the API service.
        docker (bool): A flag to indicate if ldproxy is running within a Docker environment and connecting to a database outside docker.
        config (dict): The configuration dictionary for the provider.

    Methods:
        map_datatype(data_type):
            Maps a database column datatype to a string representation for configuration.

        map_geom_type(geom_type):
            Maps a geometry type to a standardized string representation.

        create_types():
            Populates the configuration with type definitions based on table configurations.

        get_geometry_type(tablename):
            Retrieves and maps the geometry type for a table from the database schema.

        create_table_properties(columns, tablename):
            Creates properties for a table based on its columns.

        create_connection_info_dict():
            Constructs the database connection information dictionary.

        create_yaml():
            Generates a YAML file from the current configuration and exports it.
    """
    def __init__(self, service_id:str, force_axis_order:str, table_config:dict, engine, db_config:dict, docker=False):
        """
        Initializes a Provider instance.

        Args:
            service_id (str): The identifier for the service.
            force_axis_order (str): Swap lat and lon coordinates in geometry field.
            table_config (dict): The configuration of tables, including column names, column datatypes, and schema.
            engine: The SQLAlchemy engine for database connections.
            db_config (dict): The configuration dictionary with database connection details.
            docker (bool, optional): Indicates if ldproxy is running in Docker. Defaults to False.
        """
        self.service_id = service_id
        self.engine = engine
        self.docker = docker
        self.db_config = db_config
        self.table_config = table_config
        self.config = {
            "id": service_id,
            "entityStorageVersion": 2,
            "createdAt": round(time.time()),
            "lastModified": round(time.time()),
            "providerType": "FEATURE",
            "providerSubType": "SQL",
            "nativeCrs": {
                "code": 4326,
                "forceAxisOrder": force_axis_order
            },
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
        Maps a database column datatype to a specific string representation for configuration.

        Args:
            data_type: The SQLAlchemy datatype object.

        Returns:
            str: The mapped string representation of the datatype.
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
        Maps a geometry type to a standardized string representation.

        Args:
            geom_type (str): The raw geometry type as a string.

        Returns:
            str: The standardized geometry type string.
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
        Populates the configuration with type definitions for each table in the configuration.

        This method iterates over each table in the provided table configuration and creates
        properties for the columns of the table. The resulting type definitions are added to
        the provider's configuration.
        """
        for tablename in self.table_config['tables']:
            properties = self.create_table_properties(tablename['columns'], tablename['tablename'])
            self.config["types"][tablename['tablename']] = {"sourcePath": f"/{tablename['tablename']}", "properties": properties}

    def get_geometry_type(self, tablename:str):
        """
        Retrieves and maps the geometry type for a specific table from the database schema.

        Args:
            tablename (str): The name of the table.

        Returns:
            str: The geometry type as a string.
        """
        geometry_type = "ANY"

        with self.engine.connect() as connection:
            query = text("SELECT type FROM geometry_columns WHERE f_table_name = :table_name AND f_table_schema = :schema_name")
            result = connection.execute(query, {"table_name": tablename, "schema_name": self.db_config['DB_SCHEMA']})
            geom_type = result.fetchone()

            if geom_type and geom_type[0] != 'GEOMETRY':
                geometry_type = self.map_geom_type(geom_type[0])

        return geometry_type

    def create_table_properties(self, columns:list, tablename:str):
        """
        Creates a dictionary of table properties based on its columns and configurations.

        Args:
            columns (list): A list of column definitions including name and type.
            tablename (str): The name of the table.

        Returns:
            dict: A dictionary mapping column names to their configuration properties.
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
        Generates a dictionary containing database connection details.

        Returns:
            dict: A dictionary with the following keys:
                - dialect (str): The database dialect (set to 'PGIS').
                - database (str): The database name.
                - host (str): The database host, using 'host.docker.internal' if running in Docker.
                - user (str): The database username.
                - password (str): The Base64-encoded database password.
                - schemas (list): A list containing the database schema.
        """
        connection = {}
        connection['dialect'] = 'PGIS'
        connection['database'] = self.db_config['DATABASE']
        connection['host'] = 'host.docker.internal' if self.docker else self.db_config['DB_HOST']
        connection['user'] = self.db_config['DB_USER']
        password = self.db_config['DB_PASSWORD']
        connection['password'] = base64.b64encode(password.encode()).decode()
        connection['schemas'] = [self.db_config['DB_SCHEMA']]

        return connection

    def create_yaml(self):
        """
        Generates a YAML file from the current configuration and exports it.

        This method creates the necessary directories if they don't exist and generates a YAML file
        based on the current configuration. The file is saved to the `export/providers` directory.
        """
        export_path = os.path.join(os.getcwd(), 'export/providers')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.service_id}.yml")

        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)



