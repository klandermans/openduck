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


def _get_windows_clipboard() -> str:
    """Read Unicode text from Windows clipboard using Win32 API via ctypes."""
    import ctypes
    from ctypes import wintypes

    CF_UNICODETEXT = 13
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32

    user32.OpenClipboard.argtypes = [wintypes.HWND]
    user32.OpenClipboard.restype = wintypes.BOOL
    user32.GetClipboardData.argtypes = [wintypes.UINT]
    user32.GetClipboardData.restype = wintypes.HANDLE
    user32.CloseClipboard.argtypes = []
    user32.CloseClipboard.restype = wintypes.BOOL

    kernel32.GlobalLock.argtypes = [wintypes.HGLOBAL]
    kernel32.GlobalLock.restype = wintypes.LPVOID
    kernel32.GlobalUnlock.argtypes = [wintypes.HGLOBAL]
    kernel32.GlobalUnlock.restype = wintypes.BOOL

    if not user32.OpenClipboard(None):
        return ""
    try:
        handle = user32.GetClipboardData(CF_UNICODETEXT)
        if not handle:
            return ""
        ptr = kernel32.GlobalLock(handle)
        if not ptr:
            return ""
        try:
            text = ctypes.wstring_at(ptr)
            return text.replace("\r\n", "\n")
        finally:
            kernel32.GlobalUnlock(handle)
    finally:
        user32.CloseClipboard()


def get_system_clipboard() -> str:
    """Read text from system clipboard (macOS pbpaste, wl-paste/xclip on Linux, Win32 API on Windows)."""
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
        elif sys.platform == "win32":
            return _get_windows_clipboard()
    except Exception as e:
        logger.error(f"Failed to read from system clipboard: {e}")
    return ""
