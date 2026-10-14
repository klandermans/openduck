"""Database connections explorer tree with table search."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, Tree


class DatabaseTree(Vertical):
    """Tree widget showing database connections and their tables with search functionality."""

    def __init__(self):
        super().__init__(id="db-tree")
        self.connections_data = {}
        self.original_tree = Tree("Databases", id="db-tree-inner")
        self.search_input = Input(placeholder="Search tables...", id="db-search-input")

    def compose(self) -> ComposeResult:
        yield self.search_input
        yield self.original_tree

    def on_mount(self):
        self.original_tree.root.expand()
        self._ensure_add_node()

    def _ensure_add_node(self):
        """Make sure the '+ Add connection' node is always the last leaf."""
        for child in list(self.original_tree.root.children):
            if child.data and child.data.get("type") == "add_connection":
                child.remove()
        self.original_tree.root.add_leaf("+ Add connection", data={"type": "add_connection"})

    def add_connection_node(self, conn_info: dict, tables: list = None):
        conn_id = conn_info["id"]
        type_icon = "\U0001f42c" if conn_info["type"] == "mysql" else "\U0001f537"
        label = f"{type_icon} {conn_info['display_name']}"
        self.remove_connection_node(conn_id)
        # Store tables in connection info for table filtering
        conn_info["tables"] = tables or []
        self.connections_data[conn_id] = conn_info
        conn_node = self.original_tree.root.add(
            label,
            data={"type": "connection", "conn_id": conn_id},
            expand=True,
        )
        if tables:
            for table_name in sorted(tables):
                conn_node.add_leaf(
                    f"  {table_name}",
                    data={"type": "table", "conn_id": conn_id, "table": table_name, "database": conn_info["database"]},
                )
        else:
            conn_node.add_leaf("Loading...", data={"type": "loading", "conn_id": conn_id})
        self._ensure_add_node()

    def remove_connection_node(self, conn_id: str):
        for child in list(self.original_tree.root.children):
            if child.data and child.data.get("conn_id") == conn_id:
                child.remove()
                break
        self.connections_data.pop(conn_id, None)
        self._ensure_add_node()

    def filter_tables(self, search_term: str):
        """Filter the tree to show only tables that match the search term."""
        search_term = search_term.strip().lower()

        # Clear the tree and rebuild with filtered content
        self.original_tree.clear()

        for conn_id, conn_info in self.connections_data.items():
            type_icon = "\U0001f42c" if conn_info["type"] == "mysql" else "\U0001f537"
            label = f"{type_icon} {conn_info['display_name']}"

            all_tables = conn_info.get("tables", [])
            if search_term:
                filtered_tables = [table for table in all_tables if search_term in table.lower()]
            else:
                filtered_tables = all_tables

            conn_node = self.original_tree.root.add(
                label,
                data={"type": "connection", "conn_id": conn_id},
                expand=True,
            )

            if filtered_tables:
                for table_name in sorted(filtered_tables):
                    conn_node.add_leaf(
                        f"  {table_name}",
                        data={"type": "table", "conn_id": conn_id, "table": table_name, "database": conn_info["database"]},
                    )
            elif search_term:
                conn_node.add_leaf(
                    "  No matching tables",
                    data={"type": "no_match", "conn_id": conn_id},
                )
            else:
                conn_node.add_leaf("Loading...", data={"type": "loading", "conn_id": conn_id})

        self._ensure_add_node()
        self.original_tree.root.expand()

    def update_tables(self, conn_id: str, tables: list):
        conn_info = self.connections_data.get(conn_id)
        if conn_info:
            self.add_connection_node(conn_info, tables)
