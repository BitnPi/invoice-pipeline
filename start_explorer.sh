#!/usr/bin/env bash
set -e
sudo docker rm -f kuzu-explorer 2>/dev/null || true
sudo docker run -d \
    --name kuzu-explorer \
    -p 8000:8000 \
    -v "$HOME/invoice-inbox/graph_db:/database/db.kuzu" \
    -e KUZU_FILE=db.kuzu \
    -e MODE=READ_ONLY \
    kuzudb/explorer:0.11.3
echo "Started. Open http://localhost:8000"
