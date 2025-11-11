"""Default type transformation functions for different system types."""

DEFAULT_TRANSFORMATIONS = {
    "Databricks": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case 'DoubleType()':
            return column_name
        case 'StringType()':
            return column_name
        case other if other.startswith('DecimalType'):
            return column_name
        case _:
            return f"CAST({column_name} AS STRING)"
''',
    
    "Netezza": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case 'DOUBLE PRECISION':
            return column_name
        case other if 'CHAR' in other:
            return f"RTRIM({column_name})"
        case other if other.startswith('NUMERIC'):
            return column_name
        case _:
            return f"CAST({column_name} AS VARCHAR(250))"
''',
    
    "Postgres": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case 'double precision' | 'real' | 'float8' | 'float4':
            return column_name
        case other if 'char' in other.lower():
            return f"RTRIM({column_name})"
        case other if other.startswith('numeric') or other.startswith('decimal'):
            return column_name
        case _:
            return f"CAST({column_name} AS TEXT)"
''',
    
    "MySQL": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type.lower():
        case 'double' | 'float' | 'real':
            return column_name
        case other if 'char' in other:
            return f"TRIM({column_name})"
        case other if other.startswith('decimal') or other.startswith('numeric'):
            return column_name
        case _:
            return f"CAST({column_name} AS CHAR)"
''',
    
    "SQLServer": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type.lower():
        case 'float' | 'real':
            return column_name
        case other if 'char' in other:
            return f"RTRIM({column_name})"
        case other if other.startswith('decimal') or other.startswith('numeric'):
            return column_name
        case _:
            return f"CAST({column_name} AS VARCHAR(250))"
''',
    
    "Generic": '''def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case _:
            return f"CAST({column_name} AS VARCHAR(250))"
'''
}


def get_default_transformation(system_kind: str) -> str:
    """
    Get the default transformation function for a system type.
    
    Args:
        system_kind: The kind/type of system (e.g., 'Databricks', 'Netezza')
    
    Returns:
        Default transformation function as a string
    """
    return DEFAULT_TRANSFORMATIONS.get(system_kind, DEFAULT_TRANSFORMATIONS["Generic"])

