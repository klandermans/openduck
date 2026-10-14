"""SQL editor widget with syntax highlighting, undo/redo, and keyboard bindings."""

import re
from textual.binding import Binding
from textual.events import Click
from textual.widgets import TextArea
from textual.widgets.text_area import Selection

from openduck.core.clipboard import copy_to_system_clipboard, get_system_clipboard
from openduck.core.sql_utils import toggle_sql_comment_lines

# Construct bindings for SqlTextArea
_sql_text_area_bindings = []
for b in TextArea.BINDINGS:
    if b.action == "cursor_line_start":
        _sql_text_area_bindings.append(Binding("home", "cursor_line_start", "Cursor line start", show=False))
    elif b.action == "select_all":
        _sql_text_area_bindings.append(Binding("f7,ctrl+a,super+a,cmd+a,command+a", "select_all", "Select all", show=False))
    elif b.action == "copy":
        _sql_text_area_bindings.append(Binding("ctrl+c,super+c,cmd+c,command+c", "copy", "Copy", show=False))
    elif b.action == "paste":
        _sql_text_area_bindings.append(Binding("ctrl+v,super+v,cmd+v,command+v", "paste", "Paste", show=False))
    elif b.action == "cut":
        _sql_text_area_bindings.append(Binding("ctrl+x,super+x,cmd+x,command+x", "cut", "Cut", show=False))
    elif b.action == "undo":
        _sql_text_area_bindings.append(Binding("ctrl+z,super+z,cmd+z,command+z", "undo", "Undo", show=False))
    elif b.action == "redo":
        _sql_text_area_bindings.append(Binding(
            "ctrl+y,super+y,cmd+y,command+y,shift+ctrl+z,ctrl+shift+z,shift+super+z,super+shift+z,shift+cmd+z,cmd+shift+z,shift+command+z,command+shift+z",
            "redo", "Redo", show=False
        ))
    else:
        _sql_text_area_bindings.append(b)

_sql_text_area_bindings.append(
    Binding(
        "f3,ctrl+slash,super+slash,cmd+slash,command+slash,ctrl+underscore,super+underscore,cmd+underscore,command+underscore,ctrl+7,super+7,ctrl+minus,super+minus",
        "toggle_comment", "Comment", show=False
    )
)


class SqlTextArea(TextArea):
    """Specialized TextArea widget for SQL input with enhanced selection & clipboard support."""

    allow_select = False
    BINDINGS = _sql_text_area_bindings

    def select_word_at(self, location: tuple[int, int]) -> None:
        """Select the word at the given (row, col) location."""
        row, col = location
        if row >= self.document.line_count:
            return
        line = self.document.get_line(row)
        if not line:
            return
        col = max(0, min(col, len(line)))

        word_char_regex = re.compile(r'\w')
        if col < len(line) and not word_char_regex.match(line[col]) and col > 0 and word_char_regex.match(line[col - 1]):
            col -= 1

        if col < len(line) and word_char_regex.match(line[col]):
            start_col = col
            while start_col > 0 and word_char_regex.match(line[start_col - 1]):
                start_col -= 1
            end_col = col
            while end_col < len(line) and word_char_regex.match(line[end_col]):
                end_col += 1
            self.selection = Selection((row, start_col), (row, end_col))
        else:
            start_col = col
            while start_col > 0 and not line[start_col - 1].isspace() and not word_char_regex.match(line[start_col - 1]):
                start_col -= 1
            end_col = col
            while end_col < len(line) and not line[end_col].isspace() and not word_char_regex.match(line[end_col]):
                end_col += 1
            if start_col < end_col:
                self.selection = Selection((row, start_col), (row, end_col))

    async def _on_click(self, event: Click) -> None:
        if event.chain == 2:
            target = self.get_target_document_location(event)
            self.select_word_at(target)
            if self.selected_text:
                copy_to_system_clipboard(self.selected_text)
                self.app._clipboard = self.selected_text
            event.stop()
            return
        elif event.chain == 3:
            cursor_row, _ = self.get_target_document_location(event)
            self.select_line(cursor_row)
            if self.selected_text:
                copy_to_system_clipboard(self.selected_text)
                self.app._clipboard = self.selected_text
            event.stop()
            return
        await super()._on_click(event)

    def _end_mouse_selection(self) -> None:
        """When mouse selection finishes, copy selected text automatically to system clipboard."""
        super()._end_mouse_selection()
        if self.selected_text:
            copy_to_system_clipboard(self.selected_text)
            self.app._clipboard = self.selected_text

    def action_copy(self) -> None:
        """Copy selected text (or current line if no selection, like VS Code) to system clipboard."""
        selected_text = self.selected_text
        if selected_text:
            copy_to_system_clipboard(selected_text)
            self.app.copy_to_clipboard(selected_text)
            self.notify("Gekopieerd naar klembord", timeout=1.5)
        else:
            row = self.cursor_location[0]
            line = self.document.get_line(row)
            if line:
                copy_to_system_clipboard(line)
                self.app.copy_to_clipboard(line)
                self.notify("Regel gekopieerd naar klembord", timeout=1.5)

    def action_cut(self) -> None:
        """Cut selected text to system clipboard."""
        selected_text = self.selected_text
        if selected_text:
            copy_to_system_clipboard(selected_text)
            self.app.copy_to_clipboard(selected_text)
            self.replace("", *self.selection)
            self.notify("Geknipt naar klembord", timeout=1.5)
        else:
            row = self.cursor_location[0]
            line = self.document.get_line(row)
            if line:
                copy_to_system_clipboard(line)
                self.app.copy_to_clipboard(line)
                self.action_delete_line()
                self.notify("Regel geknipt naar klembord", timeout=1.5)

    def action_paste(self) -> None:
        """Paste from system clipboard (macOS pbpaste / local clipboard)."""
        if self.read_only:
            return
        text = get_system_clipboard()
        if not text:
            text = self.app.clipboard
        if text:
            if result := self._replace_via_keyboard(text, *self.selection):
                self.move_cursor(result.end_location)

    def action_select_all(self) -> None:
        """Select all text in document."""
        self.select_all()

    def action_toggle_comment(self) -> None:
        """Toggle SQL comment (-- ) like in VS Code."""
        selection = self.selection
        start_row, start_col = selection.start
        end_row, end_col = selection.end

        if start_row > end_row or (start_row == end_row and start_col > end_col):
            start_row, end_row = end_row, start_row
            start_col, end_col = end_col, start_col

        if end_row > start_row and end_col == 0:
            actual_end_row = end_row - 1
        else:
            actual_end_row = end_row

        actual_end_row = min(actual_end_row, max(0, self.document.line_count - 1))
        lines = [self.document.get_line(r) for r in range(start_row, actual_end_row + 1)]
        new_lines = toggle_sql_comment_lines(lines)

        replacement = "\n".join(new_lines)
        self.replace(replacement, (start_row, 0), (actual_end_row, len(lines[-1])), maintain_selection_offset=False)

        if selection.is_empty:
            new_col = min(len(new_lines[0]), max(0, start_col + (len(new_lines[0]) - len(lines[0]))))
            self.move_cursor((start_row, new_col))
        else:
            self.selection = Selection((start_row, 0), (actual_end_row, len(new_lines[-1])))

        is_commented = any(l.lstrip().startswith("--") for l in new_lines if l.strip())
        status = "toegevoegd (--)" if is_commented else "verwijderd"
        self.notify(f"Commentaar {status}", timeout=1.2)

    def on_key(self, event) -> None:
        key = (event.key or "").lower()
        if key in ("super+z", "cmd+z", "command+z", "ctrl+z"):
            self.action_undo()
            event.stop()
            event.prevent_default()
        elif key in (
            "super+shift+z", "shift+super+z", "cmd+shift+z", "shift+cmd+z",
            "command+shift+z", "shift+command+z", "ctrl+shift+z", "shift+ctrl+z",
            "super+y", "cmd+y", "command+y", "ctrl+y"
        ):
            self.action_redo()
            event.stop()
            event.prevent_default()

    def action_undo(self) -> None:
        """Undo last action in editor."""
        self.undo()

    def action_redo(self) -> None:
        """Redo last undone action in editor."""
        self.redo()
