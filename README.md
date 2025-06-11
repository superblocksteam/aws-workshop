# AWS Workshop Database Setup

This repository contains scripts to set up and populate a database for the Superblocks AWS workshop. The database is designed to complement the workshop by providing realistic data for operations such as inventory management, sales tracking, and order processing.

## Prerequisites

- Python 3.6 or higher
- PostgreSQL database
- AWS credentials (for environment variables)

## Setup Instructions

Follow these steps to set up the database:

1. **Create a virtual environment**:

   ```bash
   python3 -m venv venv
   ```

2. **Activate the virtual environment**:

   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```

3. **Install the required Python packages**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables (if setting up your own database)**:

   - Create a `.env` file using the `cat` command:
     ```bash
     cat > .env <<EOF
     DB_USER=<your_db_user>
     DB_PASSWORD=<your_db_password>
     DB_HOST=<your_db_host>
     DB_PORT=5432
     DB_NAME=<your_db_name>
     EOF
     ```

5. **Run the setup script**:
   ```bash
   ./setup_db.sh
   ```

This script will:

- Create the necessary tables and views in the database.
- Populate the tables with sample data.

## Notes

- Ensure your PostgreSQL database is running and accessible before running the scripts.
- The `.env` file is ignored by Git for security reasons. Do not share your credentials publicly.
