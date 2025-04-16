#!/bin/bash

# Copy the file in gui directory to workspace heasarc
cp /shared_dir/pipeline/gui/index.html /home/gamma/workspace/heasarc/index.html
cp /shared_dir/pipeline/gui/explorer.js /home/gamma/workspace/heasarc/explorer.js

# Start the HEASARC HTTP server
cd /home/gamma/workspace/heasarc
# Start the HTTP server
echo "✅ Starting HEASARC HTTP server at localhost:8081."
python3 -m http.server 8081