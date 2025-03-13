class Queryables():
    def __init__(self):
        self.config = {
            "buildingBlock": "QUERYABLES",
            "enabled": True,
            "included": ['*']
        }

    def export_as_dict(self):
        return self.config
class TileMatrixSet:
    def __init__(self):
        self.config = {
            "buildingBlock": "TILE_MATRIX_SETS",
            "enabled": True
        }

    def export_as_dict(self):
        return self.config

class Tiles:
    def __init__(self, service_id, tablename):

        self.config = {
            "buildingBlock": "TILES",
            "enabled": True,
            "TileProvider": f"{service_id}-tiles",
            "tileProviderTileset": tablename
        }
    def export_as_dict(self):
        return self.config

class CRS:
    def __init__(self):
        self.config = {
            "buildingBlock": "CRS",
            "enabled": True,
            "additionalCrs": [{"code": 4258, "forceAxisOrder": "NONE"},
                              {"code": 3857, "forceAxisOrder": "NONE"}]
        }

    def export_as_dict(self):
        return self.config

class Styles:
    def __init__(self):
        self.config = {
            "buildingBlock": "STYLES",
            "enabled": True,
            "deriveCollectionStyles": True
        }
    def export_as_dict(self):
        return self.config

class Filter:
    def __init__(self):
        self.config = {
            "buildingBlock": "FILTER",
            "enabled": True,
        }
    def export_as_dict(self):
        return self.config


# Collections API's
class FEATURES_CORE:
    def __init__(self, columns):
        self.columns = columns
        self.config = {
            "buildingBlock": "FEATURES_CORE",
            "enabled": True,
            "itemType": "feature",
            "queryables": {"spatial": ['geometry'],
                           "q": self.list_column_names()}
        }
        self.list_column_names()

    def list_column_names(self):
        return [column['name'] for column in self.columns if column['name'] not in ['geom', 'id', 'created_by'] ]

    def export_as_dict(self):
        return self.config

