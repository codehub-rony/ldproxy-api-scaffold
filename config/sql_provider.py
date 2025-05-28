import time
import os
import base64
from typing import Dict, List, Any
from sqlalchemy import VARCHAR, Text, String, TIMESTAMP, text, Integer, BIGINT
from sqlalchemy.engine import Engine
from yamlmaker import generate
from ..utils.types import map_datatype, map_geometry_type

class SQLProvider:
    """
    A class to represent a provider for generating provider configuration file for ldproxy.
    """
    def __init__(
        self,
        service_id: str,
        force_axis_order: str,
        table_config: Dict,
        engine: Engine,
        db_config: Dict,
        docker: bool = False
    ):
        """
        Initialize a SQLProvider instance.

        Args:
            service_id: The identifier for the service
            force_axis_order: Coordinate order for geometries
            table_config: Configuration dictionary for tables
            engine: SQLAlchemy engine instance
            db_config: Database connection configuration
            docker: Whether running in Docker environment
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
            "connectionInfo": self._create_connection_info(),
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

        self._create_types()

    def _create_connection_info(self) -> Dict[str, Any]:
        """Create database connection information dictionary."""
        return {
            'dialect': 'PGIS',
            'database': self.db_config['DATABASE'],
            'host': 'host.docker.internal' if self.docker else self.db_config['DB_HOST'],
            'user': self.db_config['DB_USER'],
            'password': base64.b64encode(self.db_config['DB_PASSWORD'].encode()).decode(),
            'schemas': [self.db_config['DB_SCHEMA']]
        }

    def _get_geometry_type(self, tablename: str) -> str:
        """Get and map the geometry type for a table."""
        geometry_type = "ANY"
        
        with self.engine.connect() as connection:
            query = text("""
                SELECT type 
                FROM geometry_columns 
                WHERE f_table_name = :table_name 
                AND f_table_schema = :schema_name
            """)
            result = connection.execute(
                query,
                {
                    "table_name": tablename,
                    "schema_name": self.db_config['DB_SCHEMA']
                }
            )
            geom_type = result.fetchone()

            if geom_type and geom_type[0] != 'GEOMETRY':
                geometry_type = map_geometry_type(geom_type[0])

        return geometry_type

    def _create_table_properties(self, columns: List[Dict], tablename: str) -> Dict[str, Dict]:
        """Create properties dictionary for a table's columns."""
        properties = {}
        has_datetime_role = False

        for column in columns:
            column_definition = {
                'sourcePath': f"{column['name']}",
                "type": map_datatype(column['type'])
            }

            if column['name'] == 'geom':
                column_definition.update({
                    'type': "GEOMETRY",
                    'role': "PRIMARY_GEOMETRY",
                    'geometryType': self._get_geometry_type(tablename)
                })
            elif column['name'] == 'id':
                column_definition.update({
                    'role': "ID",
                    'excludedScopes': ["RECEIVABLE"]
                })
            elif column_definition['type'] == 'DATETIME' and not has_datetime_role:
                column_definition['role'] = 'PRIMARY_INSTANT'
                has_datetime_role = True

            properties[column['name']] = column_definition

        return properties

    def _create_types(self) -> None:
        """Populate the configuration with type definitions."""
        for table in self.table_config['tables']:
            properties = self._create_table_properties(table['columns'], table['tablename'])
            self.config["types"][table['tablename']] = {
                "sourcePath": f"/{table['tablename']}",
                "properties": properties
            }

    def create_yaml(self, output_dir: str = "export") -> None:
        """
        Generate YAML configuration file.

        Args:
            output_dir: Directory to output the configuration file
        """
        output_path = os.path.join(output_dir, "providers")
        os.makedirs(output_path, exist_ok=True)
        generate(self.config, os.path.join(output_path, self.service_id)) 