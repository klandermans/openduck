"""Textual CSS styles for OpenDuck UI components."""

APP_CSS = """
Header { height: 2; }
HeaderTitle { text-align: center; }
#sidebar { width: 30; height: 100%; background: $surface; }
#files { height: 2fr; min-height: 8; }
#db-tree { height: 1fr; min-height: 8; }
#db-tree-inner { height: 1fr; }
#saved-queries-tree { height: 1fr; min-height: 5; }
#history-tree { height: 1fr; min-height: 5; }
#main-content { width: 1fr; }
TabbedContent { width: 100%; height: 1fr; }
SqlTextArea, TextArea { height: 35%; border-bottom: tall $primary; }
ResultsTable, DataTable { height: 1fr; }
#export-sidebar { width: 14; margin-left: 1; background: $panel; border: round $primary; padding: 0; overflow-y: auto; }
.export-btn { margin: 0; padding: 0; height: 3; width: 100%; }
#metadata-container {
    height: 1;
    background: $primary;
    color: $text;
    layout: horizontal;
}
#metadata-bar {
    width: 1fr;
    padding: 0 1;
}
#memory-bar {
    width: auto;
    padding: 0 1;
    text-align: right;
}
#menu { padding: 1 2; background: $surface; border: tall $primary; width: 50; height: auto; }
#menu-title { text-style: bold; margin-bottom: 1; }
#filter-input { margin-bottom: 1; }
Button { margin: 0 1; min-width: 10; }
AboutScreen { align: center middle; background: rgba(0, 0, 0, 0.7); }
#about-inner { width: 64; height: auto; background: $surface; border: tall $primary; padding: 2 4; }
#about-title { text-align: center; color: $accent; }
#about-footer { text-align: center; color: $text-disabled; }
#save-dialog { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
#dialog-title { text-style: bold; margin-bottom: 1; text-align: center; }
#query-preview { height: 3; border: solid $primary; padding: 1; }
#export-modal { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
#export-modal-title { text-style: bold; margin-bottom: 1; text-align: center; }
#export-filename { margin-bottom: 1; }
#db-search-input { margin-bottom: 1; }
#conn-dialog { width: 60; height: auto; background: $surface; border: tall $primary; padding: 2; }
#conn-dialog-title { text-style: bold; margin-bottom: 1; text-align: center; }
#conn-status { height: 1; margin-top: 1; }
AddConnectionDialog { align: center middle; background: rgba(0, 0, 0, 0.7); }
"""

MATRIX_CSS = """
MatrixScreen { background: black; }
#matrix-canvas { width: 100%; height: 100%; background: black; }
"""

ABOUT_CSS = """
#easter-egg { color: $text-disabled; }
#duck-art { color: $accent; }
"""

RESIZE_CSS = """
ResizeHandle {
    width: 1;
    height: 100%;
    background: $primary;
}
ResizeHandle:hover {
    background: $accent;
}
"""
