"""Unit tests for SqlTextArea indent and unindent behavior."""

import unittest
from textual.app import App
from textual.widgets.text_area import Selection

from openduck.gui.widgets.sql_editor import SqlTextArea


class SqlEditorTestApp(App):
    def __init__(self, initial_text: str):
        super().__init__()
        self.initial_text = initial_text

    def compose(self):
        yield SqlTextArea(
            self.initial_text,
            id="editor",
            tab_behavior="indent",
        )


class TestSqlEditorIndent(unittest.IsolatedAsyncioTestCase):
    """Test indent and unindent functionality in SqlTextArea."""

    async def test_tab_with_selection_indents_code(self):
        """Test that selecting code followed by Tab indents the lines rather than deleting."""
        app = SqlEditorTestApp("SELECT *\nFROM users\nWHERE id = 1;")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            # Select lines 0 and 1
            editor.selection = Selection((0, 0), (2, 0))
            await pilot.press("tab")

            self.assertEqual(
                editor.text,
                "    SELECT *\n    FROM users\nWHERE id = 1;",
            )
            self.assertEqual(editor.selection, Selection((0, 0), (2, 0)))

            # Tab again to increase indentation
            await pilot.press("tab")
            self.assertEqual(
                editor.text,
                "        SELECT *\n        FROM users\nWHERE id = 1;",
            )

    async def test_shift_tab_unindents_code(self):
        """Test that Shift+Tab unindents selected code."""
        app = SqlEditorTestApp("        SELECT *\n        FROM users\nWHERE id = 1;")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            editor.selection = Selection((0, 0), (2, 0))
            await pilot.press("shift+tab")

            self.assertEqual(
                editor.text,
                "    SELECT *\n    FROM users\nWHERE id = 1;",
            )

            await pilot.press("shift+tab")
            self.assertEqual(
                editor.text,
                "SELECT *\nFROM users\nWHERE id = 1;",
            )

    async def test_tab_without_selection_inserts_spaces(self):
        """Test that pressing Tab without a selection still inserts spaces at the cursor."""
        app = SqlEditorTestApp("SELECT *")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            editor.selection = Selection.cursor((0, 0))
            await pilot.press("tab")

            self.assertEqual(editor.text, "    SELECT *")

    async def test_shift_tab_without_selection_unindents_line(self):
        """Test that Shift+Tab without a selection unindents the current line."""
        app = SqlEditorTestApp("    SELECT *")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            editor.selection = Selection.cursor((0, 4))
            await pilot.press("shift+tab")

            self.assertEqual(editor.text, "SELECT *")
            self.assertEqual(editor.cursor_location, (0, 0))

    async def test_single_line_selection_indent(self):
        """Test selecting text on a single line and pressing Tab indents the line."""
        app = SqlEditorTestApp("SELECT 1;")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            editor.selection = Selection((0, 7), (0, 9))
            await pilot.press("tab")

            self.assertEqual(editor.text, "    SELECT 1;")
            self.assertEqual(editor.selection, Selection((0, 11), (0, 13)))

    async def test_reverse_selection_indent(self):
        """Test that backward (reversed) selection properly indents and retains direction."""
        app = SqlEditorTestApp("SELECT 1;\nSELECT 2;\nSELECT 3;")
        async with app.run_test() as pilot:
            editor = app.query_one(SqlTextArea)
            editor.focus()

            # Reversed selection: anchor at (2, 0), cursor at (0, 0)
            editor.selection = Selection((2, 0), (0, 0))
            await pilot.press("tab")

            self.assertEqual(
                editor.text,
                "    SELECT 1;\n    SELECT 2;\nSELECT 3;",
            )
            self.assertEqual(editor.selection, Selection((2, 0), (0, 0)))


if __name__ == "__main__":
    unittest.main()
