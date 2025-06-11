import os
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timedelta
import random

# Load environment variables
load_dotenv()

# Initialize Faker
fake = Faker()

# Set to track already used SKUs
used_skus = set()

def generate_sku():
    """Generate a realistic paper product SKU, ensuring uniqueness"""
    categories = ['CP', 'RC', 'GL', 'CV']  # Copy, Recycled, Gloss, Cover
    
    while True:
        sku = f"DM-{random.choice(categories)}-{random.randint(1000, 9999)}"
        if sku not in used_skus:
            used_skus.add(sku)
            return sku

def generate_paper_product():
    """Generate realistic paper product data"""
    categories = [
        'Copy Paper', 'Card Stock', 'Recycled Paper', 
        'Glossy Paper', 'Cover Paper', 'Bond Paper',
        'Premium Paper', 'Letterhead', 'Color Paper'
    ]
    
    locations = ['Scranton', 'Buffalo', 'Utica', 'Albany', 'Syracuse', 'Nashua']
    sheet_sizes = ['8.5x11', '11x17', 'A4', 'Legal', '12x18']
    
    current_stock = random.randint(0, 1000)
    reorder_point = random.randint(50, 200)
    
    return {
        'sku': generate_sku(),
        'product_name': f"{fake.company()} {random.choice(categories)}",
        'category_name': random.choice(categories),
        'location_name': random.choice(locations),
        'current_stock': current_stock,
        'unit_weight_lbs': round(random.uniform(1.0, 50.0), 2),
        'unit_price': round(random.uniform(20.0, 200.0), 2),
        'reorder_point': reorder_point,
        'reorder_quantity': reorder_point * 2,
        'paper_weight_gsm': random.choice([75, 90, 100, 120, 160, 200]),
        'sheet_size': random.choice(sheet_sizes),
        'sheets_per_ream': random.choice([250, 500, 1000]),
        'brightness': random.randint(84, 98),
        'is_recycled': random.choice([True, False]),
        'last_restock_date': fake.date_time_between(start_date='-1y', end_date='now'),
        'status': 'Low Stock' if current_stock <= reorder_point else 'In Stock'
    }

def populate_tables(inventory_count, sales_count, orders_count):
    """Populate all tables with sample data"""
    try:
        connection = psycopg2.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME')
        )

        cursor = connection.cursor()
        
        # Clear the used_skus set to ensure we start fresh
        used_skus.clear()
        
        # Populate inventory
        print(f"\nGenerating {inventory_count} inventory records...")
        for i in range(inventory_count):
            if i % 50 == 0:  # Progress indicator - changed to 50
                print(f"Processed {i} inventory records...")
                
            product = generate_paper_product()
            cursor.execute('''
                INSERT INTO dm_operations.inventory (
                    sku, product_name, category_name, location_name,
                    current_stock, unit_weight_lbs, unit_price,
                    reorder_point, reorder_quantity, paper_weight_gsm,
                    sheet_size, sheets_per_ream, brightness,
                    is_recycled, last_restock_date, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING inventory_id;
            ''', (
                product['sku'], product['product_name'],
                product['category_name'], product['location_name'],
                product['current_stock'], product['unit_weight_lbs'],
                product['unit_price'], product['reorder_point'],
                product['reorder_quantity'], product['paper_weight_gsm'],
                product['sheet_size'], product['sheets_per_ream'],
                product['brightness'], product['is_recycled'],
                product['last_restock_date'], product['status']
            ))
        
        # Get all inventory IDs for reference
        cursor.execute("SELECT inventory_id FROM dm_operations.inventory")
        inventory_ids = [row[0] for row in cursor.fetchall()]

        # Populate sales
        print(f"\nGenerating {sales_count} sales records...")
        for i in range(sales_count):
            if i % 50 == 0:  # Progress indicator - changed to 50
                print(f"Processed {i} sales records...")
                
            sale_date = fake.date_between(start_date='-1y', end_date='today')
            quantity = random.randint(1, 100)
            unit_price = round(random.uniform(20.0, 200.0), 2)
            total_amount = round(quantity * unit_price, 2)

            cursor.execute('''
                INSERT INTO dm_operations.sales 
                (inventory_id, sale_date, quantity, unit_price, total_amount, customer_name, location_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                random.choice(inventory_ids),
                sale_date,
                quantity,
                unit_price,
                total_amount,
                fake.company(),
                random.choice(['Scranton', 'Buffalo', 'Utica', 'Albany', 'Syracuse', 'Nashua'])
            ))

        # Populate orders
        print(f"\nGenerating {orders_count} pending orders...")
        for i in range(orders_count):
            if i % 50 == 0:  # Progress indicator - changed to 50
                print(f"Processed {i} orders...")
                
            order_date = fake.date_between(start_date='-30d', end_date='today')
            quantity = random.randint(1, 100)
            unit_price = round(random.uniform(20.0, 200.0), 2)
            total_amount = round(quantity * unit_price, 2)
            expected_delivery = fake.date_between(start_date='+1d', end_date='+30d')

            cursor.execute('''
                INSERT INTO dm_operations.orders 
                (inventory_id, order_date, quantity, unit_price, total_amount, 
                customer_name, status, expected_delivery_date, location_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                random.choice(inventory_ids),
                order_date,
                quantity,
                unit_price,
                total_amount,
                fake.company(),
                'Pending',
                expected_delivery,
                random.choice(['Scranton', 'Buffalo', 'Utica', 'Albany', 'Syracuse', 'Nashua'])
            ))

        connection.commit()
        print("\nAll tables populated successfully!")

        # Print summary
        cursor.execute("SELECT COUNT(*) FROM dm_operations.inventory")
        inv_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dm_operations.sales")
        sales_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dm_operations.orders WHERE status = 'Pending'")
        pending_count = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(total_amount) FROM dm_operations.sales WHERE EXTRACT(YEAR FROM sale_date) = EXTRACT(YEAR FROM CURRENT_DATE)")
        ytd_sales = cursor.fetchone()[0]

        print("\nDatabase Summary:")
        print(f"Total Inventory Items: {inv_count}")
        print(f"Total Sales Records: {sales_count}")
        print(f"Pending Orders: {pending_count}")
        print(f"YTD Sales: ${ytd_sales:,.2f}")

    except (Exception, Error) as error:
        print("Error while populating tables:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    print("Starting to populate the Acme Inc. database...")
    populate_tables(inventory_count=500, sales_count=500, orders_count=500)