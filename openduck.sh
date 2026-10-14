#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [ -f "$DIR/venv/bin/python3" ]; then
    "$DIR/venv/bin/python3" "$DIR/openduck.py" "$@"
else
    python3 "$DIR/openduck.py" "$@"
fi
