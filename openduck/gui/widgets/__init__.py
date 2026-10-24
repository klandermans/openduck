"""UI widgets for OpenDuck."""

from openduck.gui.widgets.database_tree import DatabaseTree
from openduck.gui.widgets.file_tree import DuckTree
from openduck.gui.widgets.plot_view import PlotView
from openduck.gui.widgets.query_tab import QueryTab
from openduck.gui.widgets.resize_handle import ResizeHandle
from openduck.gui.widgets.results_table import ResultsTable
from openduck.gui.widgets.sql_editor import SqlTextArea

__all__ = [
    "DatabaseTree",
    "DuckTree",
    "PlotView",
    "QueryTab",
    "ResizeHandle",
    "ResultsTable",
    "SqlTextArea",
]
