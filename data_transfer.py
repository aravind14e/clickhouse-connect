"""
Data transfer operations between ClickHouse and flat files
"""
import pandas as pd
from typing import List, Optional, Dict, Any
import clickhouse_connect
from clickhouse_connect.driver.client import Client
import flatfile_handler

def clickhouse_to_flatfile(
    client: Client,
    table: str,
    file_path: str,
    delimiter: str,
    columns: List[str]
) -> int:
    """
    Transfer data from ClickHouse to a flat file
    
    Args:
        client: ClickHouse client
        table: Source table name
        file_path: Target file path
        delimiter: Delimiter for the flat file
        columns: List of columns to transfer
        
    Returns:
        Number of records transferred
    """
    # Build the query
    columns_str = ", ".join(columns)
    query = f"SELECT {columns_str} FROM {table}"
    
    # Query the data in batches
    result = client.query(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result.result_rows, columns=[col[0] for col in result.column_names])
    
    # Write to flat file
    record_count = flatfile_handler.write_to_flatfile(df, file_path, delimiter, columns)
    
    return record_count

def flatfile_to_clickhouse(
    file_path: str,
    delimiter: str,
    client: Client,
    table: str,
    columns: List[str]
) -> int:
    """
    Transfer data from a flat file to ClickHouse
    
    Args:
        file_path: Source file path
        delimiter: Delimiter character
        client: ClickHouse client
        table: Target table name
        columns: List of columns to transfer
        
    Returns:
        Number of records transferred
    """
    # Read data from flat file
    df = flatfile_handler.read_from_flatfile(file_path, delimiter, columns)
    
    # Get column types from the first few rows
    column_types = {}
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            column_types[col] = 'Int64'
        elif pd.api.types.is_float_dtype(df[col]):
            column_types[col] = 'Float64'
        elif pd.api.types.is_datetime64_dtype(df[col]):
            column_types[col] = 'DateTime'
        elif pd.api.types.is_bool_dtype(df[col]):
            column_types[col] = 'UInt8'
        else:
            column_types[col] = 'String'
    
    # Check if table exists
    result = client.query(f"EXISTS TABLE {table}")
    table_exists = result.result_rows[0][0]
    
    # Create table if it doesn't exist
    if not table_exists:
        columns_def = []
        for col in df.columns:
            columns_def.append(f"`{col}` {column_types[col]}")
        
        columns_def_str = ", ".join(columns_def)
        create_table_query = f"CREATE TABLE {table} ({columns_def_str}) ENGINE = MergeTree() ORDER BY tuple()"
        client.query(create_table_query)
    
    # Insert data in batches
    batch_size = 10000
    total_rows = len(df)
    
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i+batch_size]
        client.insert_df(table, batch)
    
    return total_rows

def execute_join_transfer(
    client: Client,
    tables: List[str],
    join_keys: Dict[str, str],
    selected_columns: List[str],
    file_path: str,
    delimiter: str
) -> int:
    """
    Execute a JOIN query and transfer results to a flat file
    
    Args:
        client: ClickHouse client
        tables: List of tables to join
        join_keys: Dictionary mapping table names to join columns
        selected_columns: List of columns to select in format "table.column"
        file_path: Target file path
        delimiter: Delimiter for the flat file
        
    Returns:
        Number of records transferred
    """
    # Execute the JOIN query
    from clickhouse_handler import execute_join_query
    df = execute_join_query(client, tables, join_keys, selected_columns)
    
    # Write to flat file
    record_count = flatfile_handler.write_to_flatfile(df, file_path, delimiter)
    
    return record_count