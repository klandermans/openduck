import subprocess
import platform
from textual.app import App, ComposeResult
from textual.widgets import Static

class AboutScreen(App):
    def compose(self) -> ComposeResult:
        yield Static("Bertatron Data Systems v1.0\n\n[Druk op 'm' voor een muziekje]")

    def action_play_midi(self) -> None:
        file_path = "easter_egg.mid"
        system = platform.system()

        try:
            if system == "Darwin":  # macOS
                subprocess.Popen(["afplay", file_path])
            elif system == "Linux":
                # Vereist meestal 'aplaymidi' of 'timidity', vaak al aanwezig
                subprocess.Popen(["aplaymidi", "--port", "14", file_path])
            elif system == "Windows":
                # Start de default player (bijv. Windows Media Player)
                subprocess.Popen(["start", file_path], shell=True)
        except Exception:
            pass # Silent fail voor een grapje

    BINDINGS = [("m", "play_midi", "Play Music")]

if __name__ == "__main__":
    AboutScreen().run()
