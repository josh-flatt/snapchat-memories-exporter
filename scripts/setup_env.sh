#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Create the virtual environment in the current directory
echo "Checking for existing virtual environment..."
if [ -d "venv" ]; then
    echo "Virtual environment already exists."
else
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "Virtual environment created."
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate
echo "Virtual environment activated."

# Upgrade pip to the latest version
echo "Upgrading pip..."
pip install --upgrade pip
echo "Pip upgraded."

# Install required packages from requirements.txt
if [ -f "requirements.txt" ]; then
    echo "Installing required packages from requirements.txt..."
    pip install -r requirements.txt
    echo "Required packages installed."
else
    echo "No requirements.txt file found. Skipping package installation."
fi
echo "Setup complete."