"""Cross-platform system clipboard integration."""

import logging
import subprocess
import sys

logger = logging.getLogger(__name__)


def copy_to_system_clipboard(text: str) -> bool:
    """Copy text to system clipboard (macOS pbcopy, xclip/wl-copy on Linux, clip on Windows)."""
    if not text:
        return False
    try:
        if sys.platform == "darwin":
            p = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE, text=True)
            p.communicate(input=text)
            return True
        elif sys.platform.startswith("linux"):
            for cmd in (
                ["wl-copy"],
                ["xclip", "-selection", "clipboard"],
                ["xsel", "--clipboard", "--input"],
            ):
                try:
                    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
                    p.communicate(input=text)
                    return True
                except FileNotFoundError:
                    continue
        elif sys.platform == "win32":
            p = subprocess.Popen(["clip"], stdin=subprocess.PIPE, text=True)
            p.communicate(input=text)
            return True
    except Exception as e:
        logger.error(f"Failed to copy to system clipboard: {e}")
    return False


def get_system_clipboard() -> str:
    """Read text from system clipboard (macOS pbpaste, wl-paste/xclip on Linux)."""
    try:
        if sys.platform == "darwin":
            return subprocess.check_output(["pbpaste"], text=True)
        elif sys.platform.startswith("linux"):
            for cmd in (
                ["wl-paste"],
                ["xclip", "-selection", "clipboard", "-o"],
                ["xsel", "--clipboard", "--output"],
            ):
                try:
                    return subprocess.check_output(cmd, text=True)
                except FileNotFoundError:
                    continue
    except Exception as e:
        logger.error(f"Failed to read from system clipboard: {e}")
    return ""
