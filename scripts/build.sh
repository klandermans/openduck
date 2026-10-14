#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "Building OpenDuck with PyInstaller..."
pyinstaller --onefile --name openduck openduck.py

# Optional macOS DMG creation:
# create-dmg \
#   --volname "OpenDuck" \
#   --window-pos 200 120 \
#   --window-size 600 400 \
#   --icon-size 100 \
#   --icon "duckcli" 200 190 \
#   --hide-extension "duckcli" \
#   "OpenDuck.dmg" \
#   "dist/"

# Optional Nuitka compilation:
# pip install nuitka
# python -m nuitka --standalone --macos-create-app-bundle openduck.py
