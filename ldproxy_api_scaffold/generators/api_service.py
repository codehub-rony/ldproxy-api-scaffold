import time
from ldproxy_api_scaffold.generators.api_blocks import Queryables, TileMatrixSet, Tiles, Styles, CRS, Filter, FEATURES_CORE, Projections
import os
import yaml

class ApiService:
    """
    Represents an LDProxy service configuration generator.

    This class constructs a full service configuration including general settings,
    API building blocks, and dataset collections. The configuration can be exported
    as a YAML file for use in LDProxy.

    Attributes:
        service_id (str): Unique identifier for the service.
        api_buildingsblocks (list): List of API building blocks to include (e.g., 'TILES', 'CRS').
        table_config (dict): Dictionary containing table schema and column information.
        config (dict): The full configuration dictionary that will be exported.

    Methods:
        create_api_buildingblocks():
            Appends the selected API building blocks to the service configuration.

        create_collections():
            Adds collection configurations based on the provided table schema.

        create_yaml(export_dir: str):
            Exports the final configuration as a YAML file to the given directory.
    """

    def __init__(self, service_id:str, table_config:dict, api_buildingblocks:list):
        """
        Initializes the ApiService with basic settings and triggers configuration setup.

        Args:
            service_id (str): Unique name/identifier for the LDProxy service.
            table_config (dict): Dictionary containing table and column metadata.
            api_buildingblocks (list): List of building blocks to include in the API configuration.
        """
        self.service_id = service_id
        self.api_buildingsblocks = api_buildingblocks
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

        self.create_api_buildingblocks()
        self.create_collections()

    def create_api_buildingblocks(self):
        """
        Appends the configured API building blocks to the service.

        Based on the input list `api_buildingblocks`, this method appends instances
        of building blocks like Queryables, Tiles, CRS, etc., to the `config['api']` list.
        """

        for api in self.api_buildingsblocks:

            if (api == 'QUERYABLES'):
                self.config["api"].append(Queryables().export_as_dict())

            if (api == 'PROJECTIONS'):
                self.config['api'].append(Projections().export_as_dict())

            if (api == 'TILES'):
                self.config["api"].append(TileMatrixSet().export_as_dict())
                self.config["api"].append(Tiles(self.service_id).export_as_dict())

            if (api == 'CRS'):
                self.config["api"].append(CRS().export_as_dict())

            if (api == 'STYLES'):
                self.config["api"].append(Styles().export_as_dict())

            if (api == 'FILTER'):
                self.config["api"].append(Filter().export_as_dict())


    def create_collections(self):
        """
        Populates the `collections` section of the configuration.

        Each table in `table_config['tables']` becomes a collection. If 'FILTER' is enabled
        in the building blocks, then a `FEATURES_CORE` block with queryable columns is added
        to the collection definition.
        """
        for table in self.table_config['tables']:
            table_name = table['tablename']
            self.config["collections"][table_name] = {"id": table_name, "label": table_name, "enabled": True}

            if 'FILTER' in self.api_buildingsblocks:
                self.config["collections"][table_name]['api'] = [FEATURES_CORE(table['columns']).export_as_dict()]


    def create_yaml(self, export_dir:str):
        """
        Exports the current service configuration to a YAML file.

        The file is saved in a subdirectory `services/` under the provided export directory.

        Args:
            export_dir (str): Relative or absolute path to the export directory.
        """
        export_path = os.path.join(os.getcwd(), export_dir, 'services')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.service_id}.yml")
        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)


