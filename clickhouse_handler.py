"""
ClickHouse connection and operations handler
"""
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import List, Dict, Any, Optional
import pandas as pd

def connect_to_clickhouse(
    host: str, 
    port: int, 
    database: str, 
    user: str,
    jwt_token: str
) -> Client:
    """
    Connect to ClickHouse with JWT token authentication
    
    Args:
        host: ClickHouse server hostname
        port: ClickHouse server port
        database: Database name
        user: Username
        jwt_token: JWT token for authentication
        
    Returns:
        ClickHouse client object
    """
    # Determine if using https based on port
    secure = port in (8443, 9440)
    
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        database=database,
        user=user,
        password=jwt_token,  # Use JWT token as password
        secure=secure,
        verify=False if secure else True  # Disable SSL verification for development
    )
    
    # Test connection
    client.query("SELECT 1")
    
    return client

def get_tables(client: Client) -> List[str]:
    """
    Get list of available tables in the connected database
    
    Args:
        client: ClickHouse client
        
    Returns:
        List of table names
    """
    result = client.query("SHOW TABLES")
    return [row[0] for row in result.result_rows]

def get_table_columns(client: Client, table: str) -> List[Dict[str, str]]:
    """
    Get columns information for a table
    
    Args:
        client: ClickHouse client
        table: Table name
        
    Returns:
        List of column information dictionaries
    """
    result = client.query(f"DESCRIBE TABLE {table}")
    columns = []
    
    for row in result.result_rows:
        columns.append({
            "name": row[0],
            "type": row[1]
        })
    
    return columns

def preview_data(
    client: Client, 
    table: str, 
    columns: List[str], 
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Preview data from a ClickHouse table
    
    Args:
        client: ClickHouse client
        table: Table name
        columns: List of column names to include
        limit: Maximum number of rows to return
        
    Returns:
        List of data rows as dictionaries
    """
    columns_str = ", ".join(columns)
    query = f"SELECT {columns_str} FROM {table} LIMIT {limit}"
    
    result = client.query(query)
    column_names = [col[0] for col in result.column_names]
    
    data = []
    for row in result.result_rows:
        data.append(dict(zip(column_names, row)))
    
    return data

def execute_join_query(
    client: Client,
    tables: List[str],
    join_keys: Dict[str, str],
    selected_columns: List[str],
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Execute a JOIN query across multiple tables
    
    Args:
        client: ClickHouse client
        tables: List of tables to join
        join_keys: Dictionary mapping table names to join columns
        selected_columns: List of columns to select in format "table.column"
        limit: Optional limit on number of rows
        
    Returns:
        DataFrame with query results
    """
    # Build the SELECT clause
    select_clause = ", ".join(selected_columns)
    
    # Build the FROM and JOIN clauses
    from_clause = f"FROM {tables[0]}"
    
    for i in range(1, len(tables)):
        from_clause += f" JOIN {tables[i]} ON {tables[0]}.{join_keys[tables[0]]} = {tables[i]}.{join_keys[tables[i]]}"
    
    # Build the complete query
    query = f"SELECT {select_clause} {from_clause}"
    
    if limit:
        query += f" LIMIT {limit}"
    
    # Execute the query
    result = client.query(query)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(result.result_rows, columns=[col[0] for col in result.column_names])
    
    return df