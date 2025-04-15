"""
Flat file handling operations
"""
import pandas as pd
from typing import List, Dict, Any, Optional
import os
import csv

def get_flatfile_columns(file_path: str, delimiter: str) -> List[Dict[str, str]]:
    """
    Get column information from a flat file
    
    Args:
        file_path: Path to the flat file
        delimiter: Delimiter character
        
    Returns:
        List of column information dictionaries
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Read first few rows to determine columns
    df = pd.read_csv(file_path, delimiter=delimiter, nrows=5)
    
    columns = []
    for col in df.columns:
        # Infer data type
        data_type = str(df[col].dtype)
        columns.append({
            "name": col,
            "type": data_type
        })
    
    return columns

def preview_data(
    file_path: str, 
    delimiter: str, 
    columns: List[str], 
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Preview data from a flat file
    
    Args:
        file_path: Path to the flat file
        delimiter: Delimiter character
        columns: List of column names to include
        limit: Maximum number of rows to return
        
    Returns:
        List of data rows as dictionaries
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Read the data
    df = pd.read_csv(file_path, delimiter=delimiter, nrows=limit)
    
    # Filter columns if specified
    if columns and len(columns) > 0:
        df = df[columns]
    
    # Convert to list of dictionaries
    return df.to_dict(orient='records')

def write_to_flatfile(
    data: pd.DataFrame, 
    file_path: str, 
    delimiter: str, 
    columns: Optional[List[str]] = None
) -> int:
    """
    Write data to a flat file
    
    Args:
        data: DataFrame containing data to write
        file_path: Path to output file
        delimiter: Delimiter character
        columns: Optional list of columns to include
        
    Returns:
        Number of records written
    """
    # Filter columns if specified
    if columns and len(columns) > 0:
        data = data[columns]
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    
    # Write to CSV file
    data.to_csv(file_path, sep=delimiter, index=False)
    
    # Return the number of records written
    return len(data)

def read_from_flatfile(
    file_path: str, 
    delimiter: str, 
    columns: Optional[List[str]] = None, 
    batch_size: int = 10000
) -> pd.DataFrame:
    """
    Read data from a flat file with batching support
    
    Args:
        file_path: Path to the flat file
        delimiter: Delimiter character
        columns: Optional list of columns to include
        batch_size: Number of rows to read at once
        
    Returns:
        DataFrame containing the data
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Use pandas read_csv with specified columns and batch size
    if columns and len(columns) > 0:
        df = pd.read_csv(file_path, delimiter=delimiter, usecols=columns, chunksize=batch_size)
    else:
        df = pd.read_csv(file_path, delimiter=delimiter, chunksize=batch_size)
    
    # Return DataFrame or concatenated chunks
    if isinstance(df, pd.DataFrame):
        return df
    else:
        # Concatenate chunks into a single DataFrame
        return pd.concat(df)