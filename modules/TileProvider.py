
import os
from typing import Dict, List
import yaml

class TileProvider:

    def __init__(self, service_id:str, table_config: Dict):
        """
        Initialize a TileProvider instance.

        Args:
            service_id: The identifier for the service
            table_config: Configuration dictionary for tables
        """

        self.id = service_id
        self.table_config = table_config
        self.config = {
            "id": f"{service_id}-tiles",
            "providerType": 'TILE',
            "providerSubType": "FEATURES",
            "caches": [{"type": "IMMUTABLE",
                        "storage": "MBTILES",
                        "levels": {
                            "WebMercatorQuad": {"min": 5, "max": 12}
                            }
                        },
                        {"type": "DYNAMIC",
                        "storage": "MBTILES",
                        "seeded": False,
                        "levels": {
                            "WebMercatorQuad": {"min": 13, "max": 18}
                            }
                        }],
            "tilesetDefaults": {},
            "tilesets": {}
        }
        self.create_default_tileset_levels()
        self.create_tilesets()
    def create_default_tileset_levels(self):
        self.config["tilesetDefaults"]["levels"] =  {"WebMercatorQuad": {"min": 5, "max": 20}}


    def create_tilesets(self):
        self.config["tilesets"]["__all__"] = {"id": "__all__", "combine": ["*"]}

        for table in self.table_config['tables']:
            self.config["tilesets"][table['tablename']] = {"id": table['tablename']}


    def create_yaml(self):
        """
        Generates a YAML file from the current configuration and exports it.

        This method creates the necessary directories if they don't exist and generates a YAML file
        based on the current configuration. The file is saved to the `export/providers` directory.
        """
        export_path = os.path.join(os.getcwd(), 'export/providers')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.id}-tiles.yml")
        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)

        with open(f"export/providers/{self.id}-tiles.yml", "r+") as file:
            yaml_content = file.read()
            yaml_content = yaml_content.replace("combine:\n    - '*'", 'combine: ["*"]')
            file.seek(0)
            file.write(yaml_content)
            file.truncate()



