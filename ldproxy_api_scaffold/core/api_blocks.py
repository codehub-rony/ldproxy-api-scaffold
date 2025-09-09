class BaseBuildingBlock():
    """
    Base class for defining LDProxy building block configuration objects.

    This class serves as the foundation for all LDProxy API building blocks, providing
    common functionality and structure for configuration objects.

    Attributes:
        config (dict): Configuration dictionary representing the building block.
            Contains at minimum 'buildingBlock' and 'enabled' keys.
    """
    def __init__(self, building_block_name, params=None):
        """
        Initialize a new building block configuration.

        Args:
            building_block_name (str): The name of the building block (e.g., 'TILES', 'CRS').
        """
        self.config = {
            "buildingBlock": building_block_name,
            "enabled": True
        }
        if params:
            self.config.update(params[building_block_name])

    def export_as_dict(self):
        """
        Export the building block configuration as a dictionary.

        Returns:
            dict: The complete configuration dictionary for this building block.
        """
        return self.config

class Queryables(BaseBuildingBlock):
    """
    Configuration for queryable properties in the API.

    This building block defines which properties can be queried in the API.
    By default, it includes all properties ('*').
    """
    def __init__(self):
        super().__init__("QUERYABLES")
        self.config["included"] = ['*']

    def export_as_dict(self):
        return self.config

class HTML(BaseBuildingBlock):
    """
     Configuration for HTML output settings in the API.

     This building block defines how the API should handle or generate HTML content.
    """
    def __init__(self, params=None):
        super().__init__("HTML", params)

    def export_as_dict(self):
        return self.config

class TileMatrixSet(BaseBuildingBlock):
    """
    Configuration for tile matrix sets in the API.

    This building block defines the available tile matrix sets for tiled data.
    """
    def __init__(self):
        super().__init__("TILE_MATRIX_SETS")

    def export_as_dict(self):
        return self.config

class Tiles(BaseBuildingBlock):
    """
    Configuration for tile-based data access in the API.

    This building block enables tile-based access to data, linking to a specific
    tile provider and tileset.

    Args:
        service_id (str): The identifier of the service to link tiles to.
    """
    def __init__(self, service_id):
        super().__init__("TILES")
        self.config.update({
            "TileProvider": f"{service_id}-tiles",
            "tileProviderTileset": "__all__"
        })

class CRS(BaseBuildingBlock):
    """
    Configuration for Coordinate Reference Systems in the API.

    This building block defines the available coordinate reference systems.
    By default, it includes EPSG:4258 and EPSG:3857 with no forced axis order.
    """
    def __init__(self):
        super().__init__("CRS")
        self.config.update({
            "additionalCrs": [
                {"code": 4258, "forceAxisOrder": "NONE"},
                {"code": 3857, "forceAxisOrder": "NONE"}
            ]
        })

class Projections(BaseBuildingBlock):
    """
    Configuration for map projections in the API.

    This building block enables projection capabilities for the API.
    """
    def __init__(self):
        super().__init__("PROJECTIONS")

class Styles(BaseBuildingBlock):
    """
    Configuration for styling capabilities in the API.

    This building block enables style derivation for collections.
    By default, it enables automatic style derivation.
    """
    def __init__(self):
        super().__init__("STYLES")
        self.config.update({
             "deriveCollectionStyles": True
        })

class Filter(BaseBuildingBlock):
    """
    Configuration for filtering capabilities in the API.

    This building block enables filtering operations on API resources.
    """
    def __init__(self):
        super().__init__("FILTER")

# Collections API's
class FEATURES_CORE(BaseBuildingBlock):
    """
    Configuration for core features API capabilities.

    This building block enables the core features API functionality, including
    spatial and property-based querying.

    Args:
        columns (list): List of column definitions for the feature collection.
    """
    def __init__(self, columns):
        super().__init__("FEATURES_CORE")
        self.columns = columns
        self.config.update({
            "buildingBlock": "FEATURES_CORE",
            "enabled": True,
            "itemType": "feature",
            "queryables": {"spatial": ['geometry'],
                           "q": self.list_column_names()}
        })

    def list_column_names(self):
        """
        Get a list of queryable column names, excluding system columns.

        Returns:
            list: List of column names that can be used in queries.
        """
        return [column['name'] for column in self.columns if column['name'] not in ['geom', 'id', 'created_by'] ]


