import time
import os
from typing import Dict, List
from yamlmaker import generate

class TileProvider:
    """
    A class to represent a tile provider for generating tile configuration files for ldproxy.
    """
    def __init__(self, service_id: str, table_config: Dict):
        """
        Initialize a TileProvider instance.

        Args:
            service_id: The identifier for the service
            table_config: Configuration dictionary for tables
        """
        self.service_id = service_id
        self.table_config = table_config
        self.config = {
            "id": f"{service_id}-tiles",
            "entityStorageVersion": 2,
            "createdAt": round(time.time()),
            "lastModified": round(time.time()),
            "providerType": "TILE",
            "providerSubType": "MVT",
            "tileMatrixSet": "WebMercatorQuad",
            "collections": self._create_collections()
        }

    def _create_collections(self) -> Dict[str, Dict]:
        """
        Create collections configuration for tile provider.
        
        Returns:
            Dict containing collections configuration
        """
        collections = {}
        for table in self.table_config['tables']:
            table_name = table['tablename']
            collections[table_name] = {
                "id": table_name,
                "enabled": True,
                "tileMatrixSet": "WebMercatorQuad",
                "tileMatrixSetLimits": self._get_tile_matrix_set_limits()
            }
        return collections

    def create_yaml(self, output_dir: str = "export") -> None:
        """
        Generate YAML configuration file.

        Args:
            output_dir: Directory to output the configuration file
        """
        output_path = os.path.join(output_dir, "providers")
        os.makedirs(output_path, exist_ok=True)

        generate(self.config, os.path.join(output_path, f"{self.service_id}-tiles")

        with open(os.path.join(output_path, f"{self.service_id}-tiles"), "r+") as file:
            yaml_content = file.read()
            yaml_content = yaml_content.replace("combine:\n    - '*'", 'combine: ["*"]')
            file.seek(0)
            file.write(yaml_content)
            file.truncate()
