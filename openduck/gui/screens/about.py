"""About OpenDuck modal dialog screen."""

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Middle, Vertical
from textual.events import Click
from textual.screen import ModalScreen
from textual.widgets import Static

from openduck.gui.screens.matrix import MatrixScreen
from openduck.gui.styles import ABOUT_CSS

LOGO_RAW = (
    "   ___                       _            _    \n"
    "  / _ \\ _ __   ___ _ __   __| |_   _  ___| | __\n"
    " | | | | '_ \\ / _ \\ '_ \\ / _` | | | |/ __| |/ /\n"
    " | |_| | |_) |  __/ | | | (_| | |_| | (__|   < \n"
    "  \\___/| .__/ \\___|_| |_|\\__,_|\\__,_|\\___|_|\\_\\\n"
    "       |_|                                     "
)

LOLCAT_COLORS = [
    "#ff0000", "#ff4400", "#ff8800", "#ffcc00", "#ffff00", "#88ff00",
    "#00ff00", "#00ff88", "#00ffff", "#0088ff", "#0000ff", "#4400ff",
    "#8800ff", "#cc00ff", "#ff00ff", "#ff0088",
]


def lolcat(text: str) -> Text:
    """Apply rainbow colors to text, Rich markup style."""
    rich_text = Text(text)
    lines = text.split("\n")
    pos = 0
    for row, line in enumerate(lines):
        for col, ch in enumerate(line):
            if ch != " ":
                color = LOLCAT_COLORS[(col + row * 3) % len(LOLCAT_COLORS)]
                rich_text.stylize(color, pos, pos + 1)
            pos += 1
        pos += 1  # newline
    return rich_text


DUCK_ART = r"""                  (o.                   H
      _o)         |  . :             (o]H
  \\\__/      \\\_|  : :.        \\\_\  H
  <____).....<_____).:.::.......<_____).H"""


class AboutScreen(ModalScreen):
    """About dialog screen with ASCII duck and Easter egg link."""

    DEFAULT_CSS = ABOUT_CSS

    def compose(self) -> ComposeResult:
        with Middle():
            with Vertical(id="about-inner"):
                yield Static(lolcat(LOGO_RAW), id="about-title")
                yield Static("────────────────────────────────────────────────────")
                yield Static("[b]GitHub:[/b] github.com/bertatron/duckcli")
                yield Static("[b]Author:[/b] Bertatron")
                yield Static("[b]License:[/b] MIT")
                yield Static("\nKISS - Metadata & Async enabled.")
                yield Static("Don't click here", id="easter-egg")
                yield Static(DUCK_ART, id="duck-art")
                yield Static("\n[Press any key to close]", id="about-footer")

    def on_key(self) -> None:
        self.dismiss()

    def on_click(self, event: Click) -> None:
        if hasattr(event, 'widget') and event.widget and event.widget.id == "easter-egg":
            self.dismiss()
            self.app.push_screen(MatrixScreen())
        else:
            try:
                widget, _ = self.screen.get_widget_at(event.screen_x, event.screen_y)
                if widget.id == "easter-egg":
                    self.dismiss()
                    self.app.push_screen(MatrixScreen())
                    return
            except Exception:
                pass
            self.dismiss()
