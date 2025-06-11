import os
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_tables():
    """Create all necessary tables for the Acme Incorporated operations"""
    try:
        connection = psycopg2.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME')
        )

        cursor = connection.cursor()

        # Create schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS dm_operations;")

        # Drop existing views and tables if they exist
        print("Dropping existing views if they exist...")
        cursor.execute("DROP VIEW IF EXISTS dm_operations.pending_orders;")
        cursor.execute("DROP VIEW IF EXISTS dm_operations.sales_velocity;")
        cursor.execute("DROP VIEW IF EXISTS dm_operations.inventory_location_status;")
        
        print("Dropping existing tables if they exist...")
        cursor.execute("DROP TABLE IF EXISTS dm_operations.orders;")
        cursor.execute("DROP TABLE IF EXISTS dm_operations.sales;")
        cursor.execute("DROP TABLE IF EXISTS dm_operations.inventory;")

        # Create inventory table
        create_inventory_table = '''
            CREATE TABLE dm_operations.inventory (
                inventory_id SERIAL PRIMARY KEY,
                sku VARCHAR(50) UNIQUE NOT NULL,
                product_name VARCHAR(200) NOT NULL,
                category_name VARCHAR(100) NOT NULL,
                location_name VARCHAR(100) NOT NULL,
                current_stock INTEGER NOT NULL DEFAULT 0,
                unit_weight_lbs DECIMAL(5,2),
                unit_price DECIMAL(10,2) NOT NULL,
                reorder_point INTEGER NOT NULL,
                reorder_quantity INTEGER NOT NULL,
                paper_weight_gsm INTEGER,
                sheet_size VARCHAR(50),
                sheets_per_ream INTEGER DEFAULT 500,
                brightness INTEGER,
                is_recycled BOOLEAN DEFAULT false,
                last_restock_date TIMESTAMP,
                status VARCHAR(20) CHECK (status IN ('In Stock', 'Low Stock', 'Out of Stock', 'Discontinued')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
        
        # Create sales table
        create_sales_table = '''
            CREATE TABLE dm_operations.sales (
                sale_id SERIAL PRIMARY KEY,
                inventory_id INTEGER REFERENCES dm_operations.inventory(inventory_id),
                sale_date DATE NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                customer_name VARCHAR(200),
                location_name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''

        # Create orders table
        create_orders_table = '''
            CREATE TABLE dm_operations.orders (
                order_id SERIAL PRIMARY KEY,
                inventory_id INTEGER REFERENCES dm_operations.inventory(inventory_id),
                order_date DATE NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                customer_name VARCHAR(200),
                status VARCHAR(50) CHECK (status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled')),
                expected_delivery_date DATE,
                location_name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''

        # Execute create table queries
        print("Creating inventory table...")
        cursor.execute(create_inventory_table)
        print("Creating sales table...")
        cursor.execute(create_sales_table)
        print("Creating orders table...")
        cursor.execute(create_orders_table)
        
        # Create View 1: Stock level analysis by location
        create_inventory_location_view = '''
            CREATE OR REPLACE VIEW dm_operations.inventory_location_status AS
            SELECT 
                i.inventory_id, i.sku, i.product_name, i.category_name,
                i.location_name, i.current_stock, i.reorder_point, i.reorder_quantity,
                i.unit_price, i.unit_weight_lbs,
                (i.current_stock - i.reorder_point) AS stock_margin,
                CASE 
                    WHEN i.current_stock <= i.reorder_point THEN 'Below Reorder Point'
                    WHEN i.current_stock <= (i.reorder_point * 1.2) THEN 'Near Reorder Point'
                    ELSE 'Adequate Stock'
                END AS stock_status
            FROM dm_operations.inventory i;
        '''
        
        # Create View 2: Sales velocity by product and location
        create_sales_velocity_view = '''
            CREATE OR REPLACE VIEW dm_operations.sales_velocity AS
            SELECT 
                inventory_id, location_name,
                COUNT(sale_id) AS num_sales,
                SUM(quantity) AS total_quantity_sold,
                SUM(total_amount) AS total_revenue,
                SUM(quantity) / 90.0 AS daily_velocity  -- Fixed to exactly 90 days for consistency
            FROM dm_operations.sales
            WHERE sale_date >= '2025-01-17 00:00:00+00'::timestamp - INTERVAL '90 days'
              AND sale_date <= '2025-01-17 00:00:00+00'::timestamp
            GROUP BY inventory_id, location_name;
        '''
        
        # Create View 3: Pending orders by location
        create_pending_orders_view = '''
            CREATE OR REPLACE VIEW dm_operations.pending_orders AS
            SELECT 
                inventory_id, location_name,
                COUNT(order_id) AS num_orders,
                SUM(quantity) AS total_quantity_ordered
            FROM dm_operations.orders
            WHERE order_date >= '2025-01-17 00:00:00+00'::timestamp - INTERVAL '30 days'
              AND order_date <= '2025-01-17 00:00:00+00'::timestamp
              AND expected_delivery_date >= '2025-01-17 00:00:00+00'::timestamp
            GROUP BY inventory_id, location_name;
        '''
        
        # Execute view creation queries
        print("Creating inventory_location_status view...")
        cursor.execute(create_inventory_location_view)
        print("Creating sales_velocity view...")
        cursor.execute(create_sales_velocity_view)
        print("Creating pending_orders view...")
        cursor.execute(create_pending_orders_view)
        
        connection.commit()
        print("All tables and views created successfully!")

    except (Exception, Error) as error:
        print("Error while creating tables and views:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    print("Starting Acme Inc database setup...")
    create_tables()