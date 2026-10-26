# 🦆 OpenDuck

<p align="center">
  <b>A flat file SQL client based on DuckDB</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9+" />
  <img src="https://img.shields.io/badge/DuckDB-Fast%20Analytics-yellow?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/TUI-Textual-green?style=for-the-badge" alt="Textual" />
  <img src="https://img.shields.io/badge/License-MIT-orange?style=for-the-badge" alt="MIT License" />
</p>

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

**macOS / Linux (via PyPI):**
```bash
pip3 install openduck-cli
```

**Windows (PowerShell / Command Prompt):**
```powershell
pip install openduck-cli
# or via the Windows Python Launcher:
py -m pip install openduck-cli
```

> **Tip (macOS / Linux PEP 668):** If your system manages Python externally (e.g., Debian/Ubuntu/Homebrew), install into a virtual environment or use `pipx`:
> ```bash
> python3 -m venv ~/.openduck-venv
> ~/.openduck-venv/bin/pip3 install openduck-cli
> # Create a symlink to run it from anywhere:
> sudo ln -s ~/.openduck-venv/bin/openduck /usr/local/bin/openduck
> ```


**From source:**
```bash
git clone https://github.com/klandermans/openduck.git
cd openduck
pip3 install -r requirements.txt
# or install in editable mode:
pip3 install -e .
```

> **Note:** `textual[syntax]` installs the required `tree-sitter` and `tree-sitter-sql` engines for syntax highlighting out of the box (supports Python 3.9 – 3.14 across macOS, Linux, and Windows).

---

### 2. Run OpenDuck

After installing from PyPI, run it directly from any directory:

```bash
openduck
```
*(or `openduck-cli`, or `py -m openduck` on Windows if `Scripts` is not in your `PATH`)*

If you cloned the source repository:

```bash
python3 openduck.py
# or
python3 -m openduck
# or with the shell wrapper:
./openduck.sh
```

---

## ⌨️ Keyboard & Mouse Shortcuts

| Shortcut | Description |
| :--- | :--- |
| `Ctrl+Enter` / `Cmd+Enter` | **Run Query** (runs selection if text is selected, or full editor) |
| `Ctrl+S` / `Cmd+S` | **Save Query** with custom name to `openduck.json` |
| `F3` / `Cmd+/` / `Ctrl+/` | **Toggle SQL Comment** (`--`) on current line or selection |
| `Tab` / `Shift+Tab` | **Indent / Unindent** selected SQL lines |
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

## 📁 Project Structure

```text
openduck/
├── openduck/                 # Core Python package
│   ├── __init__.py           # Package entry and exports
│   ├── __main__.py           # CLI invocation (python -m openduck)
│   ├── main.py               # Application bootstrap & logging setup
│   ├── core/                 # Core logic & backend
│   │   ├── config.py         # Config persistence, query history, saved queries
│   │   ├── clipboard.py      # Cross-platform system clipboard integration
│   │   ├── sql_utils.py      # SQL formatting, file-to-query mapper, comments
│   │   └── error_classifier.py # Diagnostic error classifier
│   └── gui/                  # Textual user interface
│       ├── app.py            # Main DuckCLI application
│       ├── styles.py         # Textual CSS stylesheets
│       ├── screens/          # Modal screens (About, Matrix screensaver)
│       ├── dialogs/          # Modal dialogs (Save Query, Export, Add DB)
│       └── widgets/          # Custom widgets (Editor, ResultsTable, Trees, etc.)
├── docs/                     # Documentation & guides
├── scripts/                  # Build scripts & packaging specs
├── tests/                    # Automated test suite
├── samples/                  # Sample datasets for testing & exploration
├── openduck.py               # Root executable script
├── openduck.sh               # Shell launcher script
└── pyproject.toml            # Python package configuration
```

---

## 📜 License


<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-F6SG49ZW15"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-F6SG49ZW15');
</script>

Distributed under the **MIT License**. See `LICENSE` for details.

