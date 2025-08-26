import os
from typing import Dict, List
import yaml

class TileProvider:
    """
    A class to represent a tile provider configuration for ldproxy.

    This class manages the configuration for tile-based data access in ldproxy,
    including tile caching strategies, zoom levels, and tileset definitions.
    It supports both immutable (pre-generated) and dynamic tile caching.

    Attributes:
        id (str): The identifier for the tile service.
        table_config (dict): Configuration dictionary containing table definitions.
        config (dict): The complete configuration dictionary for the tile provider.
            Contains settings for caches, tileset defaults, and individual tilesets.

    Methods:
        create_default_tileset_levels():
            Sets default zoom level ranges for tile generation.

        create_tilesets():
            Creates individual tileset entries for each table and a combined '__all__' tileset.

        create_yaml(export_dir):
            Generates and exports the tile provider YAML file.
    """

    def __init__(self, service_id:str, table_config: Dict):
        """
        Initialize a TileProvider instance.

        Args:
            service_id (str): The identifier for the service.
            table_config (dict): A configuration dictionary for tables, including their names.
                Should contain a 'tables' key with a list of table definitions.
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
        """
        Sets default zoom level range for all tilesets.

        Adds a 'levels' key to 'tilesetDefaults' in the configuration, which defines
        the range of WebMercatorQuad zoom levels used by default. The default range
        is from zoom level 5 to 20, covering most common use cases.
        """
        self.config["tilesetDefaults"]["levels"] =  {"WebMercatorQuad": {"min": 5, "max": 20}}

    def create_tilesets(self):
        """
        Creates tileset definitions for each table and a global '__all__' tileset.

        Each table in the configuration gets its own tileset, identified by the table name.
        A special '__all__' tileset is created to combine all individual tables into one
        tileset, which is useful for applications that need to display all data together.

        The '__all__' tileset uses a wildcard ('*') to include all available tables.
        """
        self.config["tilesets"]["__all__"] = {"id": "__all__", "combine": ["*"]}

        for table in self.table_config['tables']:
            self.config["tilesets"][table['tablename']] = {"id": table['tablename']}

    def create_yaml(self, export_dir:str):
        """
        Generates a YAML file from the current configuration and exports it.

        This method creates the necessary directories if they don't exist, then
        writes the current tile provider configuration to a YAML file. It also
        post-processes the file to properly format the 'combine' field for the
        '__all__' tileset.

        Args:
            export_dir (str): The directory where the 'providers' folder will be created.
                The final YAML file will be saved in a 'providers' subdirectory.
        """
        export_path = os.path.join(export_dir, 'providers')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.id}-tiles.yml")
        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)

        # Post-process the YAML file to fix the combine field format
        with open(yaml_file, "r+") as file:
            yaml_content = file.read()
            yaml_content = yaml_content.replace("combine:\n    - '*'", 'combine: ["*"]')
            file.seek(0)
            file.write(yaml_content)
            file.truncate()



