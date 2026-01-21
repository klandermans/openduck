# DuckCLI

A lightweight, high-performance TUI (Terminal User Interface) for **DuckDB**, built with Python and Textual. Navigate, preview, and query data files (Parquet, CSV, SQLite, JSON, Excel) directly from your terminal.
<img width="2378" height="818" alt="image" src="https://github.com/user-attachments/assets/92d6c9ae-2055-49b1-bb72-c000832a5661" />

## Features

* **Filtered File Browser**: Automatically displays only DuckDB-compatible files.
* **SQL Editor**: Built-in syntax highlighting for SQL queries.
* **Interactive Results**: Sortable `DataTable` (click column headers) with zebra striping for readability.
* **Smart Preview**: Automatically generates the correct `SELECT`, `READ_X`, or `ATTACH` statements based on file extension.
* **Query History**: Persists your last executed query in `duckcli_history.sql`.
* **Context Menu**: Right-click files to quickly `DESCRIBE` or insert paths into the editor.

## Installation

1. Clone the repository:
   ```bash
   git clone [https://github.com/yourusername/duckcli.git](https://github.com/yourusername/duckcli.git)
   cd duckcli# openduck
