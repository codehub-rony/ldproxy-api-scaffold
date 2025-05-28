class BaseBuildingBlock():
    def __init__(self, building_block_name):
        self.config = {
            "buildingBlock": building_block_name,
            "enabled": True
        }

    def export_as_dict(self):
        return self.config

class Queryables(BaseBuildingBlock):
    def __init__(self):
        super().__init__("QUERYABLES")
        self.config["included"] = ['*']

    def export_as_dict(self):
        return self.config

class TileMatrixSet(BaseBuildingBlock):
    def __init__(self):
        super().__init__("TILE_MATRIX_SETS")

    def export_as_dict(self):
        return self.config

class Tiles(BaseBuildingBlock):
    def __init__(self, service_id):
        super().__init__("TILES")
        self.config.update({
            "TileProvider": f"{service_id}-tiles",
            "tileProviderTileset": "__all__"
        })

class CRS(BaseBuildingBlock):
    def __init__(self):
        super().__init__("CRS")
        self.config.update({
            "additionalCrs": [
                {"code": 4258, "forceAxisOrder": "NONE"},
                {"code": 3857, "forceAxisOrder": "NONE"}
            ]
        })

class Projections(BaseBuildingBlock):
    def __init__(self):
        super().__init__("PROJECTIONS")


class Styles(BaseBuildingBlock):
    def __init__(self):
        super().__init__("STYLES")
        self.config.update({
             "deriveCollectionStyles": True
        })

class Filter(BaseBuildingBlock):
    def __init__(self):
        super().__init__("FILTER")

# Collections API's
class FEATURES_CORE(BaseBuildingBlock):
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
        return [column['name'] for column in self.columns if column['name'] not in ['geom', 'id', 'created_by'] ]


