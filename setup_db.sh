#!/bin/bash

# Script to set up and populate the Acme Inc operations database
# This script runs the table creation script followed by the data population script

echo "==============================================="
echo "      ACME INC DATABASE SETUP           "
echo "==============================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not found"
    exit 1
fi

# Install required Python packages
echo "Installing required Python packages..."
pip3 install -q -r requirements.txt

# Create .env file with environment variables from AWS
echo "Setting up database connection..."
cat > .env << EOF
DB_USER=$DB_USERNAME
DB_PASSWORD=$DB_PASSWORD
DB_HOST=$DB_ENDPOINT
DB_PORT=5432
DB_NAME=$DB_NAME
EOF

# Run the table creation script
echo -e "\n[1/2] Creating database tables and views..."
python3 create_tables_views.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to create tables and views"
    exit 1
fi

# Run the data population script
echo -e "\n[2/2] Populating tables with sample data..."
python3 populate_tables.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to populate tables"
    exit 1
fi

echo -e "\n==============================================="
echo "      DATABASE SETUP COMPLETE                  "
echo "==============================================="