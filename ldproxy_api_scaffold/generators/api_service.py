import time
from ldproxy_api_scaffold.generators.api_blocks import Queryables, TileMatrixSet, Tiles, Styles, CRS, Filter, FEATURES_CORE, Projections
import os
import yaml

class ApiService:
    """
    A class to represent a service that generates configuration files for ldproxy, including API building blocks
    and collections based on provided table configuration.

    Attributes:
        service_id (str): The identifier for the service.
        api_buildingblocks (list): A list of API building blocks to be included in the configuration.
        table_config (dict): The configuration dictionary for tables, including columns names, column datatypes, and schema.
        config (dict): The configuration dictionary for the service, including API buildinglbocks and collections.

    Methods:
        create_api_buildingblocks():
            Populates the API configuration with the specified API building blocks.

        create_collections():
            Populates the collections in the configuration based on the table configuration.

        create_yaml():
            Generates a YAML file from the current configuration and exports it.
    """
    def __init__(self, service_id:str, table_config:dict, api_buildingblocks:list):
        """
        Initializes a Service instance.

        Args:
            service_id (str): The identifier for the service.
            table_config (dict): The configuration of tables, including columns names, column datatypes, and schema.
            api_buildingblocks (list): A list of API building blocks to be included in the service configuration.
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
        Populates the API configuration with the specified API building blocks (e.g., Queryables, Tiles, CRS, Styles, Filter).
        Each building block is added to the 'api' list in the configuration.
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
        Populates the collections in the configuration based on the table configuration.
        Each table defined in the configuration is added as a collection, with the table name as the collection ID.
        If the 'FILTER' API building block is included, the core filter configuration is added to each collection.
        """
        for table in self.table_config['tables']:
            table_name = table['tablename']
            self.config["collections"][table_name] = {"id": table_name, "label": table_name, "enabled": True}

            if 'FILTER' in self.api_buildingsblocks:
                self.config["collections"][table_name]['api'] = [FEATURES_CORE(table['columns']).export_as_dict()]


    def create_yaml(self, export_dir:str):
        """
        Generates a YAML file from the current configuration and exports it to the 'export/services' directory.
        The file is named based on the service ID.
        """
        export_path = os.path.join(os.getcwd(), export_dir, 'services')

        if not os.path.exists(export_path):
          os.makedirs(export_path)

        yaml_file = os.path.join(export_path, f"{self.service_id}.yml")
        with open(yaml_file, 'w') as f:
            yaml.dump(self.config, f, sort_keys=False)


