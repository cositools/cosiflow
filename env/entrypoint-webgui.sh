#!/bin/bash

# Start the HEASARC HTTP server
cd /home/gamma/workspace/heasarc
# Start the HTTP server
echo "✅ Starting HEASARC HTTP server at localhost:8081."
python3 -m http.server 8081