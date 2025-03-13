import time
from yamlmaker import generate
import os

class TileProvider:

    def __init__(self, service_id:str, table_config:dict):
        """
        A class for generating a tile provider configuration in YAML format.

        This class creates a tile provider configuration based on table settings and exports it as a YAML file.

        Attributes:
        id (str): The identifier for the service.
        table_config (dict): The configuration of tables, including table names and settings.
        config (dict): The complete configuration for the tile provider.
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
        print(self.table_config)

        for table in self.table_config['tables']:
            self.config["tilesets"][table['tablename']] = {"id": table['tablename']}


    def create_yaml(self):
        """
        Generates a YAML file from the current configuration and exports it.

        This method creates the necessary directories if they don't exist and generates a YAML file
        based on the current configuration. The file is saved to the `export/providers` directory.
        """
        current_folder_path = os.path.join(os.getcwd(), 'export/providers')

        if not os.path.exists(current_folder_path):
          os.makedirs(current_folder_path)

        generate(self.config, f"export/providers/{self.id}-tiles")



