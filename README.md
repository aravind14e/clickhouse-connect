# ClickHouse & Flat File Bidirectional Data Ingestion Tool

A web-based application that facilitates bidirectional data transfer between ClickHouse databases and flat files. This tool supports JWT token-based authentication for ClickHouse, allows column selection, and reports the total number of records processed.

## Features

- **Bidirectional Data Flow**:
  - ClickHouse to Flat File
  - Flat File to ClickHouse
  
- **Authentication**:
  - JWT token-based authentication for ClickHouse connections
  
- **Data Selection**:
  - Table selection (when ClickHouse is the source)
  - Column selection with checkboxes
  
- **Monitoring**:
  - Data preview before ingestion
  - Record count reporting
  - Status updates during the process
  
- **Bonus Features**:
  - Multi-table JOIN support (when ClickHouse is the source)
  - Visual progress indication

## Prerequisites

- Python 3.8+
- ClickHouse server (local or remote)
- Sample data files or ClickHouse databases for testing

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/clickhouse-flatfile-tool.git
   cd clickhouse-flatfile-tool
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Start the application:
   ```
   python app.py
   ```

2. Open your web browser and navigate to:
   ```
   http://localhost:5000
   ```

3. Select the data flow direction:
   - ClickHouse → Flat File
   - Flat File → ClickHouse

4. Configure the source and target:
   - For ClickHouse: Provide host, port, database, user, and JWT token
   - For Flat File: Provide file path and delimiter

5. Select tables and columns:
   - Test the connection
   - Load available tables (when ClickHouse is the source)
   - Select columns for transfer

6. Preview data (optional):
   - Click the "Preview Data" button to see the first 100 records

7. Start the ingestion process:
   - Click "Start Ingestion" to begin the data transfer
   - Monitor the progress and final record count

## Testing

Use ClickHouse example datasets for testing:
- `uk_price_paid`
- `ontime`

These can be loaded into your ClickHouse instance following [ClickHouse's documentation](https://clickhouse.com/docs/getting-started/example-datasets).

## Project Structure

```
clickhouse-flatfile-ingestion-tool/
├── app.py                  # Main Flask application
├── static/
│   ├── css/
│   │   └── styles.css      # CSS styles
│   └── js/
│       └── main.js         # Frontend JavaScript
├── templates/
│   └── index.html          # Main HTML template
├── clickhouse_handler.py   # ClickHouse connection and operations
├── flatfile_handler.py     # Flat file operations
├── data_transfer.py        # Data transfer logic
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

## ClickHouse Client Usage

This project uses the [clickhouse-connect](https://github.com/aravind14e/clickhouse-connect) Python client library to interact with ClickHouse. The library handles authentication, querying, and data transfer with the ClickHouse server.

## License

MIT

## Author

Your Name