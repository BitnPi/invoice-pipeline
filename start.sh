#!/bin/bash
# Quick start script for the invoice pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Invoice Processing Pipeline${NC}"
echo "=============================="

# Check if venv exists
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv .venv
    .venv/bin/pip install -e .
fi

# Activate venv
source .venv/bin/activate

# Initialize folders if needed
if [ ! -d "$HOME/invoice-inbox" ]; then
    echo -e "${YELLOW}Initializing folder structure...${NC}"
    invoice-pipeline init
fi

# Show status
echo ""
invoice-pipeline status
echo ""

# Parse command line arguments
DAEMON=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--daemon)
            DAEMON=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Build command
CMD="invoice-pipeline run"
if [ "$DAEMON" = true ]; then
    CMD="$CMD --daemon"
fi
if [ "$VERBOSE" = true ]; then
    CMD="$CMD --verbose"
fi

echo -e "${GREEN}Starting pipeline...${NC}"
echo "Command: $CMD"
echo ""
echo "Drop files into these folders to process them:"
echo "  PDF:    ~/invoice-inbox/pdf/"
echo "  Images: ~/invoice-inbox/images/"
echo ""
echo "Press Ctrl+C to stop"
echo "=============================="
echo ""

exec $CMD
