import time
import os
from typing import List, Dict
from sqlalchemy.engine import Engine
from yamlmaker import generate
from .api_building_blocks import (
    Queryables, TileMatrixSet, Tiles, Styles, CRS, 
    Filter, FEATURES_CORE, Projections
)

class Service:
    """
    A class to represent a service that generates configuration files for ldproxy.
    """
    def __init__(
        self, 
        service_id: str, 
        table_config: Dict, 
        api_building_blocks: List[str], 
        engine: Engine
    ):
        """
        Initialize a Service instance.

        Args:
            service_id: The identifier for the service
            table_config: Configuration dictionary for tables
            api_building_blocks: List of API building blocks to include
            engine: SQLAlchemy engine instance
        """
        self.service_id = service_id
        self.api_building_blocks = api_building_blocks
        self.engine = engine
        self.table_config = table_config
        self.config = {
            "id": service_id,
            "createdAt": round(time.time()),
            "lastModified": round(time.time()),
            "entityStorageVersion": 2,
            "label": service_id,
            "description": "",
            "enabled": True,
            "serviceType": "OGC_API",
            "api": [],
            "collections": {}
        }

        self.create_api_building_blocks()
        self.create_collections()

    def create_api_building_blocks(self) -> None:
        """Populate the API configuration with specified building blocks."""
        building_blocks = {
            'QUERYABLES': Queryables,
            'PROJECTIONS': Projections,
            'TILES': lambda: [TileMatrixSet(), Tiles(self.service_id)],
            'CRS': CRS,
            'STYLES': Styles,
            'FILTER': Filter
        }

        for api in self.api_building_blocks:
            if api in building_blocks:
                block = building_blocks[api]()
                if isinstance(block, list):
                    for b in block:
                        self.config["api"].append(b.export_as_dict())
                else:
                    self.config["api"].append(block.export_as_dict())

    def create_collections(self) -> None:
        """Populate collections in the configuration based on table config."""
        for table in self.table_config['tables']:
            table_name = table['tablename']
            self.config["collections"][table_name] = {
                "id": table_name,
                "label": table_name,
                "enabled": True
            }

            if 'FILTER' in self.api_building_blocks:
                self.config["collections"][table_name]['api'] = [
                    FEATURES_CORE(table['columns']).export_as_dict()
                ]

    def create_yaml(self, output_dir: str = "export") -> None:
        """
        Generate YAML configuration file.

        Args:
            output_dir: Directory to output the configuration file
        """
        output_path = os.path.join(output_dir, "services")
        os.makedirs(output_path, exist_ok=True)
        generate(self.config, os.path.join(output_path, self.service_id)) 