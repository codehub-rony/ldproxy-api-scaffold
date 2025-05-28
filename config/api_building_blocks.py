from typing import Dict, List, Any

class APIBuildingBlock:
    """Base class for API building blocks."""
    def export_as_dict(self) -> Dict[str, Any]:
        """Export the building block configuration as a dictionary."""
        raise NotImplementedError

class Queryables(APIBuildingBlock):
    """Queryables API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "QUERYABLES",
            "enabled": True
        }

class TileMatrixSet(APIBuildingBlock):
    """TileMatrixSet API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "TILE_MATRIX_SET",
            "enabled": True,
            "tileMatrixSet": "WebMercatorQuad"
        }

class Tiles(APIBuildingBlock):
    """Tiles API building block."""
    def __init__(self, service_id: str):
        self.service_id = service_id

    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "TILES",
            "enabled": True,
            "tileMatrixSet": "WebMercatorQuad",
            "tileProvider": f"{self.service_id}-tiles"
        }

class Styles(APIBuildingBlock):
    """Styles API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "STYLES",
            "enabled": True
        }

class CRS(APIBuildingBlock):
    """CRS API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "CRS",
            "enabled": True,
            "crs": ["http://www.opengis.net/def/crs/EPSG/0/4326"]
        }

class Filter(APIBuildingBlock):
    """Filter API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "FILTER",
            "enabled": True
        }

class Projections(APIBuildingBlock):
    """Projections API building block."""
    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "PROJECTIONS",
            "enabled": True
        }

class FEATURES_CORE(APIBuildingBlock):
    """Features Core API building block."""
    def __init__(self, columns: List[Dict[str, Any]]):
        self.columns = columns

    def export_as_dict(self) -> Dict[str, Any]:
        return {
            "type": "FEATURES_CORE",
            "enabled": True,
            "properties": self._get_properties()
        }

    def _get_properties(self) -> Dict[str, Dict[str, str]]:
        """Get properties configuration for the features core."""
        properties = {}
        for column in self.columns:
            if column['name'] == 'geom':
                properties[column['name']] = {
                    "type": "GEOMETRY",
                    "role": "PRIMARY_GEOMETRY"
                }
            elif column['name'] == 'id':
                properties[column['name']] = {
                    "type": "STRING",
                    "role": "ID"
                }
            else:
                properties[column['name']] = {
                    "type": "STRING"  # Default type, can be overridden based on column type
                }
        return properties 