#!/bin/bash

# === Copy and rename raw.tar.gz with timestamp ===
SOURCE_DIR="/home/gamma/workspace/data/raw"
DEST_DIR="/home/gamma/workspace/data/tsmap"
SOURCE_FILE="$SOURCE_DIR/raw.tar.gz"

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Generate timestamp in format YYMMDD_hhmmss
TIMESTAMP=$(date +"%y%m%d_%H%M%S")
DEST_FILE="$DEST_DIR/${TIMESTAMP}.tar.gz"

# Check if source file exists
if [ -f "$SOURCE_FILE" ]; then
    echo "Copying $SOURCE_FILE to $DEST_FILE..."
    cp "$SOURCE_FILE" "$DEST_FILE"
    echo "File copied successfully: $DEST_FILE"
    echo "File size: $(du -h "$DEST_FILE" | cut -f1)"
else
    echo "Error: Source file $SOURCE_FILE not found!"
    exit 1
fi
