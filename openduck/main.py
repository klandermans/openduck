"""OpenDuck - Main application entry point."""

import logging
from openduck.gui.app import DuckCLI


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
    setup_logging()
    app = DuckCLI()
    app.run()


if __name__ == "__main__":
    main()
