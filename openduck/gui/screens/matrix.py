"""Matrix rain screensaver screen (Easter egg)."""

import random
from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.widgets import Static

from openduck.gui.styles import MATRIX_CSS

MATRIX_CHARS = "ｱｲｳｴｵｶｷｸｹｺｻｼｽｾｿﾀﾁﾂﾃﾄﾅﾆﾇﾈﾉ0123456789"
# ANSI 256: 82=#87ff00, 46=#00ff00, 40=#00d700, 34=#00af00, 28=#008700, 22=#005f00
MATRIX_SHADES = ["#87ff00", "#00ff00", "#00d700", "#00af00", "#008700", "#005f00"]


class MatrixScreen(ModalScreen):
    """Matrix code rain modal screensaver."""

    DEFAULT_CSS = MATRIX_CSS

    def __init__(self):
        super().__init__()
        self._drops = []
        self._grid = []
        self._colors = []
        self._timer = None

    def compose(self) -> ComposeResult:
        yield Static("", id="matrix-canvas")

    def on_mount(self) -> None:
        cols = self.screen.size.width
        rows = self.screen.size.height
        self._drops = [random.randint(0, rows) for _ in range(cols)]
        # Persistent grid - characters stay until overwritten
        self._grid = [[" "] * cols for _ in range(rows)]
        self._colors = [["#005f00"] * cols for _ in range(rows)]
        self._timer = self.set_interval(0.03, self._tick)

    def _tick(self) -> None:
        cols = self.screen.size.width
        rows = self.screen.size.height
        # Resize grids if terminal changed
        while len(self._drops) < cols:
            self._drops.append(0)
        self._drops = self._drops[:cols]
        while len(self._grid) < rows:
            self._grid.append([" "] * cols)
            self._colors.append(["#005f00"] * cols)
        self._grid = self._grid[:rows]
        self._colors = self._colors[:rows]
        for r in range(rows):
            while len(self._grid[r]) < cols:
                self._grid[r].append(" ")
                self._colors[r].append("#005f00")
            self._grid[r] = self._grid[r][:cols]
            self._colors[r] = self._colors[r][:cols]
        # Draw drops - characters persist on the grid like the original
        for i in range(cols):
            if random.random() > 0.97:
                self._drops[i] = 0
            for j, shade in enumerate(MATRIX_SHADES):
                y = self._drops[i] - j
                if 0 <= y < rows:
                    self._grid[y][i] = random.choice(MATRIX_CHARS)
                    self._colors[y][i] = shade
            self._drops[i] += 1
            if self._drops[i] >= rows:
                self._drops[i] = 0
        # Render
        lines = []
        for r in range(rows):
            parts = []
            for c in range(cols):
                ch = self._grid[r][c]
                if ch != " ":
                    parts.append(f"[{self._colors[r][c]}]{ch}[/]")
                else:
                    parts.append(" ")
            lines.append("".join(parts))
        self.query_one("#matrix-canvas").update("\n".join(lines))

    def on_key(self) -> None:
        if self._timer:
            self._timer.stop()
        self.dismiss()

    def on_click(self) -> None:
        if self._timer:
            self._timer.stop()
        self.dismiss()
