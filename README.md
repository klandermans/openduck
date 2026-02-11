# OpenDuck - Enhanced DuckDB CLI

OpenDuck is an enhanced command-line interface for DuckDB with a modern TUI (Text User Interface) built using Textual. It provides advanced features for data exploration and query management.

## Features

### Original Features
- **Async Execution**: Queries run in a separate thread; interface doesn't hang
- **Metadata Footer**: Shows execution time and number of results below the table
- **Multi-Tab Support**: Open multiple files or queries simultaneously
- **File Filtering**: DirectoryTree shows only supported files

### New Enhanced Features
- **Save Queries**: Save queries with names to `openduck.json`
- **Query History**: Automatic storage of query history in `openduck.json`
- **Sidebar Interface**: Left panel showing files, history, and saved queries
- **Quick Access**: Click on history or saved queries to load them instantly
- **Export Results**: Export query results to CSV or Excel files with dedicated buttons

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python openduck.py
```

### Keyboard Shortcuts
- `Ctrl+Enter`: Run current query
- `Ctrl+S`: Save current query
- `Ctrl+W`: Close current tab
- `Ctrl+A`: Show about dialog
- `Ctrl+Q`: Quit application

### Saving Queries
1. Type or modify a query in the editor
2. Press `Ctrl+S` or use the save function
3. Enter a name for your query when prompted
4. The query will appear in the "Saved Queries" section in the left panel

### Loading Queries
- Click on any entry in the "History" or "Saved Queries" sections to load it into the current editor

### Exporting Results
- After running a query, use the "Export CSV" or "Export Excel" buttons next to the results table
- Files are automatically named based on the table name in your query
- Excel export requires pandas and openpyxl (included in requirements.txt)

## Configuration

All history and saved queries are stored in `openduck.json` in the current directory:
```json
{
  "history": [
    {
      "sql": "SELECT * FROM table1;",
      "timestamp": "2026-02-10T10:30:45.123456"
    }
  ],
  "saved_queries": [
    {
      "name": "my_report_query",
      "sql": "SELECT COUNT(*) FROM sales WHERE date > '2023-01-01';",
      "timestamp": "2026-02-10T10:30:45.123456"
    }
  ]
}
```

## Supported File Formats

- `.parquet`, `.arrow`, `.duckdb`, `.csv`, `.csv.gz`
- `.json`, `.jsonl`, `.sqlite`, `.sqlite3`, `.db`
- `.xlsx`, `.xls`

## Development

The application is built with [Textual](https://textual.textualize.io/) and uses DuckDB for data processing. The UI follows a responsive layout with:

- Left sidebar containing file explorer, history, and saved queries
- Main content area with tabbed interface for multiple queries
- Bottom metadata bar showing execution statistics

## License

MIT