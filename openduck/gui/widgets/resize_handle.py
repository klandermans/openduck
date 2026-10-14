"""Draggable sidebar resize handle widget."""

from textual.events import MouseDown, MouseMove, MouseUp
from textual.widgets import Static

from openduck.gui.styles import RESIZE_CSS


class ResizeHandle(Static):
    """A draggable handle to resize the sidebar."""

    DEFAULT_CSS = RESIZE_CSS

    def __init__(self):
        super().__init__("┃")
        self._dragging = False

    def on_mouse_down(self, event: MouseDown) -> None:
        self._dragging = True
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        if self._dragging:
            # screen_x gives us the absolute position of the mouse
            screen_width = self.screen.size.width
            new_width = max(10, min(event.screen_x, screen_width - 20))
            sidebar = self.screen.query_one("#sidebar")
            sidebar.styles.width = new_width
            event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._dragging:
            self._dragging = False
            self.release_mouse()
            event.stop()
