# 🦆 OpenDuck

<p align="center">
  <b>The Modern, Lightning-Fast Terminal SQL IDE & Data Explorer for DuckDB</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.10+" />
  <img src="https://img.shields.io/badge/DuckDB-Fast%20Analytics-yellow?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/TUI-Textual-green?style=for-the-badge" alt="Textual" />
  <img src="https://img.shields.io/badge/License-MIT-orange?style=for-the-badge" alt="MIT License" />
</p>

---

<p align="center">
  <img width="100%" alt="OpenDuck Interface" src="https://github.com/user-attachments/assets/4aea9df6-6f00-494d-a171-d76a9a3b8133" />
</p>

**OpenDuck** brings the power and speed of **DuckDB** straight to your terminal in an intuitive, multi-tab Text User Interface (TUI). Effortlessly browse files, inspect datasets, write queries with full syntax highlighting, sort and filter millions of rows in milliseconds, and export results — all without leaving your shell.

---

## ✨ Features

- ⚡ **Instant File Queries**: Click or select any file (`.csv`, `.parquet`, `.xlsx`, `.json`, `.duckdb`, `.sqlite`, `.arrow`) from the sidebar to instantly load and execute queries with zero boilerplates.
- 🎨 **Rich SQL Syntax Highlighting**: Tree-sitter powered VS Code Dark styling with real-time token highlighting for SQL keywords, strings, numbers, identifiers, and comments.
- 🚀 **Asynchronous & Non-Blocking**: Queries execute in dedicated background threads — the interface remains buttery smooth and never freezes, even on heavy multi-gigabyte queries.
- 📑 **Multi-Tab Workspace**: Work with multiple datasets simultaneously with tab navigation, close buttons (`✕`), and tab titles dynamically bound to files and queries.
- 📊 **Interactive Data Viewer**:
  - Sort columns ascending/descending with clickable headers (safe null handling).
  - Quick column filters.
  - Live query execution runtime, row count, and memory consumption stats.
- 📋 **Terminal & System Clipboard Integration**:
  - Copy highlighted cells or entire rows (`Ctrl+C`).
  - Copy full query results as TSV (`Ctrl+A`).
  - Native terminal copy & paste with multi-level Undo/Redo (`Cmd+Z` / `Ctrl+Z` / `Ctrl+Y`).
  - Toggle SQL comments effortlessly (`F3` / `Cmd+/`).
- 💾 **One-Click Export**: Export results to `.csv` or `.xlsx` files with a single click.
- 📜 **History & Saved Queries**: Automatically preserves query history and lets you save reusable named queries with quick one-click recall into your editor.
- 🎛️ **Draggable Resizable Sidebar**: Adjust the width of your sidebar on the fly with mouse drag.
- 🛡️ **Intelligent Error Diagnostics**: Corrupted file, wrong delimiter, permission issue, or missing dataset? OpenDuck identifies the exact cause and displays human-readable actionable solutions.
- 🐇 **Easter Egg**: Secret Matrix rain screensaver!

---

## 📂 Supported File Formats

OpenDuck automatically recognises and formats queries for:

| Format | Extensions | Query Mode |
| :--- | :--- | :--- |
| **Parquet** | `.parquet` | Native direct query |
| **Arrow** | `.arrow` | Native direct query |
| **CSV & GZ** | `.csv`, `.csv.gz` | `read_csv_auto(...)` |
| **JSON & JSONL** | `.json`, `.jsonl` | `read_json_auto(...)` |
| **Excel** | `.xlsx`, `.xls` | In-memory table registration (`pandas` + `openpyxl`) |
| **SQLite** | `.sqlite`, `.sqlite3`, `.db` | Automatic SQLite extension attach |
| **DuckDB** | `.duckdb` | Database attach & table explore |

---

## 🚀 Quickstart

### 1. Installation

Clone the repository and install the dependencies:

```bash
git clone https://github.com/klandermans/openduck.git
cd openduck
pip install -r requirements.txt
```

> **Note:** `textual[syntax]` installs the required `tree-sitter` and `tree-sitter-sql` engines for syntax highlighting out of the box.

### 2. Run OpenDuck

```bash
python openduck.py
```

Or make it available system-wide as a command:

```bash
chmod +x openduck.sh
sudo cp openduck.sh /usr/local/bin/openduck
openduck
```

---

## ⌨️ Keyboard & Mouse Shortcuts

| Shortcut | Description |
| :--- | :--- |
| `Ctrl+Enter` / `Cmd+Enter` | **Run Query** (runs selection if text is selected, or full editor) |
| `Ctrl+S` / `Cmd+S` | **Save Query** with custom name to `openduck.json` |
| `F3` / `Cmd+/` / `Ctrl+/` | **Toggle SQL Comment** (`--`) on current line or selection |
| `Ctrl+Z` / `Cmd+Z` | **Undo** last edit |
| `Ctrl+Y` / `Cmd+Shift+Z` | **Redo** undone edit |
| `Ctrl+W` / `Cmd+W` | **Close Current Tab** |
| `Ctrl+M` | **Toggle Mouse Mode** (switch between TUI mouse and native terminal text selection) |
| `Ctrl+C` / `Cmd+C` | **Copy** (Editor selection, or table row/cell to system clipboard) |
| `Ctrl+A` / `Cmd+A` | **Copy Table** (when focused on results) / Select all (in editor) |
| `F1` | **About Dialog** (click the duck for a surprise! 🕶️) |
| `Ctrl+Q` / `Cmd+Q` | **Quit** OpenDuck |

---

## ⚙️ Configuration & Storage

OpenDuck automatically manages `openduck.json` in your working directory. It persists:
- The last 200 executed queries in chronological order.
- Your named saved queries.
- Database configurations.

```json
{
  "history": [
    {
      "sql": "SELECT * FROM read_csv_auto('sales.csv') WHERE revenue > 1000 LIMIT 100;",
      "timestamp": "2026-10-14T15:30:00.123456"
    }
  ],
  "saved_queries": [
    {
      "name": "Q3 Revenue by Region",
      "sql": "SELECT region, SUM(amount) FROM read_parquet('data/*.parquet') GROUP BY region;",
      "timestamp": "2026-10-14T15:32:00.654321"
    }
  ],
  "connections": []
}
```

---

## 💡 Tips & Tricks

- **Click to Query**: Clicking any file in the left sidebar automatically populates the query into your active tab, updates the tab name, and executes the preview.
- **Run Selection Only**: Highlight any part of your SQL script and press `Ctrl+Enter` to run only the highlighted snippet — just like in DBeaver, DataGrip, or VS Code!
- **Selective Comments**: Select multiple lines and press `F3` or `Cmd+/` to comment or uncomment SQL blocks simultaneously.
- **Instant Clipboard Sharing**: Results copied via `Ctrl+C` or `Ctrl+A` are immediately synced to your macOS (`pbcopy`), Linux (`wl-copy`/`xclip`), or Windows clipboard.

---

## 📜 License

Distributed under the **MIT License**. See `LICENSE` for details.
