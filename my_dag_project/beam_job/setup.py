#!/bin/bash

# Install Python packages using PIP
sudo apt-get update
sudo apt-get install -y python3-pip

# Optional: Upgrade pip
sudo python3 -m pip install --upgrade pip

# Install required packages passed via metadata
PACKAGES=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/PIP_PACKAGES" -H "Metadata-Flavor: Google")
sudo python3 -m pip install ${PACKAGES}
