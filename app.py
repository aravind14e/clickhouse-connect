"""
Main Flask application for ClickHouse & Flat File data ingestion tool
"""
from flask import Flask, render_template, request, jsonify
import os
import json

import clickhouse_handler
import flatfile_handler
import data_transfer

app = Flask(__name__)

@app.route('/')
def index():
    """Render the main application page"""
    return render_template('index.html')

@app.route('/api/test_clickhouse_connection', methods=['POST'])
def test_clickhouse_connection():
    """Test connection to ClickHouse"""
    connection_params = request.json
    try:
        client = clickhouse_handler.connect_to_clickhouse(
            host=connection_params['host'],
            port=connection_params['port'],
            database=connection_params['database'],
            user=connection_params['user'],
            jwt_token=connection_params['jwt_token']
        )
        return jsonify({"success": True, "message": "Connection successful"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Connection failed: {str(e)}"})

@app.route('/api/get_clickhouse_tables', methods=['POST'])
def get_clickhouse_tables():
    """Get available tables from ClickHouse"""
    connection_params = request.json
    try:
        client = clickhouse_handler.connect_to_clickhouse(
            host=connection_params['host'],
            port=connection_params['port'],
            database=connection_params['database'],
            user=connection_params['user'],
            jwt_token=connection_params['jwt_token']
        )
        tables = clickhouse_handler.get_tables(client)
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        return jsonify({"success": False, "message": f"Failed to get tables: {str(e)}"})

@app.route('/api/get_table_columns', methods=['POST'])
def get_table_columns():
    """Get columns for a ClickHouse table"""
    params = request.json
    try:
        client = clickhouse_handler.connect_to_clickhouse(
            host=params['host'],
            port=params['port'],
            database=params['database'],
            user=params['user'],
            jwt_token=params['jwt_token']
        )
        columns = clickhouse_handler.get_table_columns(client, params['table'])
        return jsonify({"success": True, "columns": columns})
    except Exception as e:
        return jsonify({"success": False, "message": f"Failed to get columns: {str(e)}"})

@app.route('/api/get_flatfile_columns', methods=['POST'])
def get_flatfile_columns():
    """Get columns from a flat file"""
    try:
        file_path = request.json['file_path']
        delimiter = request.json['delimiter']
        columns = flatfile_handler.get_flatfile_columns(file_path, delimiter)
        return jsonify({"success": True, "columns": columns})
    except Exception as e:
        return jsonify({"success": False, "message": f"Failed to get columns: {str(e)}"})

@app.route('/api/preview_data', methods=['POST'])
def preview_data():
    """Preview data from source (ClickHouse or flat file)"""
    params = request.json
    source_type = params['source_type']
    
    try:
        if source_type == 'clickhouse':
            client = clickhouse_handler.connect_to_clickhouse(
                host=params['connection']['host'],
                port=params['connection']['port'],
                database=params['connection']['database'],
                user=params['connection']['user'],
                jwt_token=params['connection']['jwt_token']
            )
            data = clickhouse_handler.preview_data(
                client, 
                params['table'], 
                params['columns'],
                limit=100
            )
            return jsonify({"success": True, "data": data})
        
        elif source_type == 'flatfile':
            data = flatfile_handler.preview_data(
                params['file_path'],
                params['delimiter'],
                params['columns'],
                limit=100
            )
            return jsonify({"success": True, "data": data})
        
        else:
            return jsonify({"success": False, "message": "Invalid source type"})
            
    except Exception as e:
        return jsonify({"success": False, "message": f"Failed to preview data: {str(e)}"})

@app.route('/api/start_ingestion', methods=['POST'])
def start_ingestion():
    """Start data ingestion process"""
    params = request.json
    source_type = params['source_type']
    target_type = params['target_type']
    
    try:
        # ClickHouse to Flat File
        if source_type == 'clickhouse' and target_type == 'flatfile':
            client = clickhouse_handler.connect_to_clickhouse(
                host=params['source_config']['host'],
                port=params['source_config']['port'],
                database=params['source_config']['database'],
                user=params['source_config']['user'],
                jwt_token=params['source_config']['jwt_token']
            )
            
            record_count = data_transfer.clickhouse_to_flatfile(
                client,
                params['source_config']['table'],
                params['target_config']['file_path'],
                params['target_config']['delimiter'],
                params['columns']
            )
            
            return jsonify({
                "success": True, 
                "message": "Data ingestion completed", 
                "record_count": record_count
            })
        
        # Flat File to ClickHouse
        elif source_type == 'flatfile' and target_type == 'clickhouse':
            client = clickhouse_handler.connect_to_clickhouse(
                host=params['target_config']['host'],
                port=params['target_config']['port'],
                database=params['target_config']['database'],
                user=params['target_config']['user'],
                jwt_token=params['target_config']['jwt_token']
            )
            
            record_count = data_transfer.flatfile_to_clickhouse(
                params['source_config']['file_path'],
                params['source_config']['delimiter'],
                client,
                params['target_config']['table'],
                params['columns']
            )
            
            return jsonify({
                "success": True, 
                "message": "Data ingestion completed", 
                "record_count": record_count
            })
        
        else:
            return jsonify({"success": False, "message": "Invalid source/target combination"})
            
    except Exception as e:
        return jsonify({"success": False, "message": f"Data ingestion failed: {str(e)}"})

if __name__ == '__main__':
    app.run(debug=True)