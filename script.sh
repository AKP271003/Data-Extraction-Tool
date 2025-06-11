#!/bin/bash

set -e

SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1

# Create a virtual environment
VENV_DIR="$SCRIPT_DIR/venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment...."
    python -m venv "$VENV_DIR"
fi

ACTIVATE_SCRIPT=""
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    ACTIVATE_SCRIPT="$VENV_DIR/Scripts/activate"
else
    ACTIVATE_SCRIPT="$VENV_DIR/bin/activate"
fi

# Activate the virtual environment
if [ -f "$ACTIVATE_SCRIPT" ]; then
    echo "Activating virtual environment...."
    source "$ACTIVATE_SCRIPT"
else
    echo "Error: Virtual environment activation script not found!"
    exit 1
fi

# Install required Python dependencies
echo "Installing dependencies..."
#CUSTOM_URL="https://abcd.com:8443"
# pip install --upgrade pip -i $CUSTOM_URL
pip install -r requirements.txt -i $CUSTOM_URL

if ! python -c "import flask" &> /dev/null; then
    echo "Flask not installed. Exiting."
    deactivate 
    exit 1
fi

echo "Generating config.txt from database schema..."
python generate_config.py

# Start the Flask API server
echo "Starting Flask API server using Waitress..."
python -m waitress --listen=0.0.0.0:5000 db_app:app

deactivate