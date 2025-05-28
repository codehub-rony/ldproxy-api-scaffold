from typing import Any
from sqlalchemy import VARCHAR, Text, String, TIMESTAMP, Integer, BIGINT

def map_datatype(data_type: Any) -> str:
    """
    Map a database column datatype to a string representation for configuration.
    
    Args:
        data_type: SQLAlchemy datatype object
        
    Returns:
        str: Mapped string representation of the datatype
    """
    if isinstance(data_type, (VARCHAR, Text, String)):
        return 'STRING'
    elif isinstance(data_type, TIMESTAMP):
        return 'DATETIME'
    elif isinstance(data_type, (BIGINT, Integer)):
        return 'INTEGER'
    else:
        return str(data_type)

def map_geometry_type(geom_type: str) -> str:
    """
    Map a geometry type to a standardized string representation.
    
    Args:
        geom_type: Raw geometry type as string
        
    Returns:
        str: Standardized geometry type string
    """
    geometry_mapping = {
        "MULTILINESTRING": "MULTI_LINE_STRING",
        "LINESTRING": "LINE_STRING",
        "MULTIPOLYGON": "MULTI_POLYGON",
        "MULTIPOINT": "MULTI_POINT"
    }
    return geometry_mapping.get(geom_type, geom_type) 