# DuckCLI

**DuckCLI** is a lightweight, high-performance Terminal User Interface (TUI) for **DuckDB**, built with Python and the Textual framework. It allows you to navigate, preview, and query data files (Parquet, CSV, SQLite, JSON, Excel) instantly without leaving your terminal.

<img width="2378" height="818" alt="image" src="https://github.com/user-attachments/assets/92d6c9ae-2055-49b1-bb72-c000832a5661" />

## Features

* **Asynchronous Execution**: Queries run in background threads; the UI remains responsive even during heavy data processing.
* **Filtered File Browser**: Automatically identifies and displays DuckDB-compatible files.
* **Multi-Tab Interface**: Open multiple files or scratchpads simultaneously.
* **Interactive Results**: Full-featured `DataTable` with zebra striping and interactive column headers for sorting and filtering.
* **Smart SQL Generation**: Automatically generates `SELECT`, `READ_X`, or `ATTACH` statements based on file extensions.
* **Metadata Insights**: Real-time stats on row counts and execution timing for every query.
* **Persistence**: Your query history is saved to `duckcli_history.sql` and the last active query is restored on startup.

## Supported Formats

* **Columnar**: `.parquet`, `.arrow`
* **Database**: `.duckdb`, `.sqlite`, `.sqlite3`, `.db`
* **Structured**: `.csv`, `.csv.gz`, `.json`, `.jsonl`
* **Spreadsheet**: `.xlsx`, `.xls`

## Keybindings

| Key | Action |
| :--- | :--- |
| `Ctrl + Enter` | Run current SQL query |
| `Ctrl + W` | Close active tab |
| `Ctrl + A` | Show About info |
| `Ctrl + Q` | Quit DuckCLI |
| `Click Header` | Sort or filter column |

## Installation

1. **Clone the repository**:
   ```bash
   git clone [https://github.com/bertatron/duckcli.git](https://github.com/bertatron/duckcli.git)
   cd duckcli