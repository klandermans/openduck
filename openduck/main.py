"""OpenDuck - Main application entry point."""

import logging
from openduck.gui.app import DuckCLI


def _increase_file_limit():
    """Increase soft limit for open files (RLIMIT_NOFILE) on Unix/macOS."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = min(hard, 10240) if hard != resource.RLIM_INFINITY else 10240
        if target > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except Exception:
        pass


def setup_logging():
    """Configure application logging."""
    logging.basicConfig(
        filename='openduck_debug.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        filemode='a'
    )


def main():
    """Run OpenDuck."""
    _increase_file_limit()
    setup_logging()
    app = DuckCLI()
    app.run()


if __name__ == "__main__":
    main()

