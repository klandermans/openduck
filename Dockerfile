FROM python:3.12-slim

WORKDIR /data

# Install openduck-cli directly from PyPI
RUN pip install --no-cache-dir openduck-cli

# Start OpenDuck by default
ENTRYPOINT ["openduck"]
