#!/bin/bash
# this script compresses all JSON files in the hal_embeddings/processed directory

PROCESSED_DIR="$HOME/hal_embeddings/processed"

if [ ! -d "$PROCESSED_DIR" ]; then
    echo "Directory $PROCESSED_DIR does not exist."
    exit 1
fi

cd "$PROCESSED_DIR" || exit

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

ARCHIVE_NAME="processed_files_$TIMESTAMP.tar.gz"

echo "Compressing JSON files..."

find . -name "*.json" -print0 | xargs -0 tar -cvzf "$ARCHIVE_NAME"

if [ $? -eq 0 ]; then
    find . -name "*.json" -delete
    echo "Compression successful. JSON files deleted."
else
    echo "Compression failed. JSON files were not deleted."
    exit 1
fi

cd - || exit
