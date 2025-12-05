import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# ----------------------------------------------------
# 🛠️ 1. Configuration and Constants
# ----------------------------------------------------

DB_HOST = "localhost"  # Change if using remote DB
DB_USER = "root"       # Your MySQL username
DB_PASSWORD = "Ad5021825!!" # Your MySQL password
DB_NAME = "eCommerce_DB" # The database name you created from your SQL DDL

# --- Constants for constraints ---
DAYS_FOR_FINAL_STATUS = 31 # Orders older than this must be Delivered/Refunded
DAYS_FOR_RETURN_WINDOW = 30 # Return must be initiated within X days of order date
DAYS_FOR_IN_PROCESS = 30 # Returns older than this must be 'returned'

TODAY = datetime.now().date()
CUTOFF_DATE = TODAY - timedelta(days=DAYS_FOR_FINAL_STATUS)
RECENT_RETURN_CUTOFF = TODAY - timedelta(days=DAYS_FOR_IN_PROCESS)

# Set the start and end of the entire simulation period
START_PERIOD = datetime(2024, 1, 1).date()
# End 5 days before 'now' to ensure shipping/delivery dates can be calculated after the order date
END_PERIOD = datetime.now().date() - timedelta(days=5)

# ----------------------------------------------------
# ⚙️ 2. Helper Functions
# ----------------------------------------------------

def get_random_date(start_date, end_date):
    """Returns a random date object between start_date and end_date (inclusive)."""
    # Convert date objects to datetime objects for timedelta math
    start_date_obj = datetime.combine(start_date, datetime.min.time())
    end_date_obj = datetime.combine(end_date, datetime.min.time())
    
    time_diff = end_date_obj - start_date_obj
    
    # Handle case where start_date is >= end_date
    if time_diff.days <= 0:
        return start_date_obj.date()
        
    random_days = random.randrange(time_diff.days + 1) # +1 to make end_date inclusive
    return (start_date_obj + timedelta(days=random_days)).date()

def get_random_phone_number():
    """Generates a realistic 10-digit phone number as a BIGINT (integer)."""
    return random.randint(1000000000, 9999999999) 

# ----------------------------------------------------
## 🔌 3. Connect to MySQL
# ----------------------------------------------------

try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()
    print(f"✅ Connected to MySQL database '{DB_NAME}'")
except mysql.connector.Error as err:
    print(f"❌ Error connecting to MySQL: {err}")
    exit()

# ----------------------------------------------------
## 1️⃣ Phase 1: Insert Independent Data
# ----------------------------------------------------

print("\n--- Phase 1: Inserting Independent Lookup Data ---")

# --- A. Country and City Data ---
countries = ['United States', 'Canada', 'Mexico', 'United Kingdom', 'Germany']
country_ids = {}
city_ids = {}
all_city_ids = []

for country_name in countries:
    cursor.execute("INSERT INTO Country (Name) VALUES (%s)", (country_name,))
    country_id = cursor.lastrowid
    country_ids[country_name] = country_id

    for _ in range(random.randint(2, 4)):
        city_name = fake.city()
        cursor.execute("INSERT INTO City (Name, Country_id) VALUES (%s, %s)", (city_name, country_id))
        city_id = cursor.lastrowid
        city_ids[city_name] = city_id
        all_city_ids.append(city_id)

conn.commit()
print(f"  - Inserted {len(country_ids)} countries and {len(city_ids)} cities.")

# --- B. Category Data ---
categories = ['Electronics', 'Books', 'Home & Kitchen', 'Fashion', 'Toys', 'Sports']
all_category_ids = []

for category_name in categories:
    cursor.execute("INSERT INTO Category (Name) VALUES (%s)", (category_name,))
    all_category_ids.append(cursor.lastrowid)

conn.commit()
print(f"  - Inserted {len(categories)} categories.")

# --- C. User Data ---
NUM_USERS = 1000
user_ids = []

for _ in range(NUM_USERS):
    first_name = fake.first_name().replace("'", "")
    last_name = fake.last_name().replace("'", "")
    email = fake.unique.email()
    phone = get_random_phone_number()

    cursor.execute(
        "INSERT INTO User (FirstName, LastName, Phone, Email) VALUES (%s, %s, %s, %s)",
        (first_name, last_name, phone, email)
    )
    user_ids.append(cursor.lastrowid)

conn.commit()
print(f"  - Inserted {len(user_ids)} users.")

# --- D. Address Data ---
NUM_ADDRESSES = NUM_USERS * 3
address_ids = []

for _ in range(NUM_ADDRESSES):
    address = fake.street_address().replace("'", "")
    address2 = fake.secondary_address().replace("'", "") if random.random() < 0.2 else None
    district = fake.word() if random.random() < 0.5 else None
    postal_code = random.randint(10000, 99999)
    city_id = random.choice(all_city_ids)

    cursor.execute(
        """
        INSERT INTO Address (Address, Address2, District, Postal_code, City_id)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (address, address2, district, postal_code, city_id)
    )
    address_ids.append(cursor.lastrowid)

conn.commit()
print(f"  - Inserted {len(address_ids)} addresses.")

# --- E. User_address Join Table ---
for user_id in user_ids:
    num_user_addresses = random.randint(1, 2)
    user_address_ids = random.sample(address_ids, num_user_addresses)

    for address_id in user_address_ids:
        try:
            cursor.execute(
                "INSERT INTO User_address (User_id, Address_id) VALUES (%s, %s)",
                (user_id, address_id)
            )
        except mysql.connector.IntegrityError:
            pass 

conn.commit()
print("  - Users and Addresses linked.")

# --- F. Product and Inventory Data ---
NUM_PRODUCTS = 5000
product_ids = []
product_details = {} 

for _ in range(NUM_PRODUCTS):
    product_name = fake.catch_phrase()
    unit_price = round(random.uniform(5.0, 1500.0), 2)
    category_id = random.choice(all_category_ids)

    # Insert Product
    cursor.execute(
        "INSERT INTO Product (Product_name, Unit_price, Category_id) VALUES (%s, %s, %s)",
        (product_name, unit_price, category_id)
    )
    product_id = cursor.lastrowid
    product_ids.append(product_id)
    product_details[product_id] = (product_name, unit_price)

    # Insert Inventory
    quantity = random.randint(2, 300)
    cursor.execute(
        "INSERT INTO Inventory (Product_id, Quantity) VALUES (%s, %s)",
        (product_id, quantity)
    )

conn.commit()
print(f"  - Inserted {len(product_ids)} products and inventory records.")


# ----------------------------------------------------
## 2️⃣ Phase 2: Insert Transactions (Orders, Shipping, Returns)
# ----------------------------------------------------

NUM_ORDERS = 100000
payment_methods = ['Credit_card', 'Debt_card', 'PayPal']

# Pre-calculate and sort all order dates chronologically
time_range_seconds = int((datetime.combine(END_PERIOD, datetime.min.time()) - datetime.combine(START_PERIOD, datetime.min.time())).total_seconds())
random_timestamps = sorted([random.randint(0, time_range_seconds) for _ in range(NUM_ORDERS)])
order_dates = [(datetime.combine(START_PERIOD, datetime.min.time()) + timedelta(seconds=ts)).date() for ts in random_timestamps]

print(f"\n--- Phase 2: Inserting {NUM_ORDERS} Orders with Correct Status Logic ---")

for i in range(NUM_ORDERS):
    
    order_date = order_dates[i]
    user_id = random.choice(user_ids)
    
    # Initial Decisions
    is_placed_order = order_date >= (datetime.now() - timedelta(days=3)).date()
    is_historical_order = order_date < CUTOFF_DATE
    will_have_return = random.random() < 0.10

    # Payment and Order Status Setup
    shipping_id = None
    return_id = None
    day_of_arrival = None
    order_status = 'Placed'
    payment_status = 'Paid'
    shipping_cost = 0.0

    if is_placed_order and random.random() < 0.1:
        payment_status = 'Unpaid'
    
    if will_have_return and is_historical_order:
         payment_status = 'Refunded'

    # Insert Payment
    cursor.execute(
        "INSERT INTO Payment (Payment_method, Payment_status) VALUES (%s, %s)",
        (random.choice(payment_methods), payment_status)
    )
    payment_id = cursor.lastrowid

    # Shipping Logic
    if not is_placed_order: 
        address_id = random.choice(address_ids)
        shipping_method = random.choice(['Express', 'Ground'])
        shipping_cost = round(random.uniform(5.0, 50.0), 2)
        shipping_date = order_date + timedelta(days=random.randint(0, 3)) 
        
        estimated_arrival_delay = random.randint(5, 14) 
        estimated_day_of_arrival = shipping_date + timedelta(days=estimated_arrival_delay) 
        
        is_arrived = is_historical_order or (random.random() < 0.9)
        
        if is_arrived:
            arrival_delay = random.randint(1, 14) 
            day_of_arrival = shipping_date + timedelta(days=arrival_delay) 
            order_status = 'Deliverd'
        else:
            day_of_arrival = None
            order_status = 'Shipped'
            
        
        # Insert Shipping
        cursor.execute(
            """
            INSERT INTO Shipping (Address_id, Shipping_method, Shipping_cost, Shipping_date, Estimated_day_of_arrival, Day_of_arrival)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (address_id, shipping_method, shipping_cost, shipping_date, estimated_day_of_arrival, day_of_arrival)
        )
        shipping_id = cursor.lastrowid


    # Returns Logic
    if will_have_return and day_of_arrival is not None:
        
        max_return_initiation_date = order_date + timedelta(days=DAYS_FOR_RETURN_WINDOW)
        min_return_initiation_date = day_of_arrival + timedelta(days=1)
        
        # Check for return eligibility (if arrival date is within the return window)
        if min_return_initiation_date <= max_return_initiation_date:
        
            return_initiation_date = get_random_date(min_return_initiation_date, max_return_initiation_date)

            is_recent_return = return_initiation_date >= RECENT_RETURN_CUTOFF

            if is_recent_return and random.random() < 0.2:
                return_status = 'in_process'
            else:
                return_status = 'returned'
                
            expected_refund_date = day_of_arrival + timedelta(days=random.randint(7, 30))
            refund_amount = round(random.uniform(10.0, 500.0), 2)
            reason = fake.text(max_nb_chars=100).replace("'", "")
            
            refund_date = None
            if return_status == 'returned':
                refund_date = get_random_date(expected_refund_date - timedelta(days=3), expected_refund_date + timedelta(days=14))
                if refund_date > TODAY:
                    refund_date = TODAY

                # KEY FIX: Update Order/Payment status ONLY when the return is completed
                order_status = 'Refunded'
                payment_status = 'Refunded' 
                
                cursor.execute(
                    "UPDATE Payment SET Payment_status = %s WHERE Payment_id = %s",
                    (payment_status, payment_id)
                )

            # Insert Returns
            cursor.execute(
                """
                INSERT INTO Returns (Return_status, Refund_amount, Reason, Expected_refund_date, Refund_date, Payment_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (return_status, refund_amount, reason, expected_refund_date, refund_date, payment_id)
            )
            return_id = cursor.lastrowid
        
        # If the return check failed (arrival too late), ensure status is delivered
        elif is_historical_order:
            order_status = 'Deliverd'


    # FINAL STATUS OVERRIDE for old orders that didn't refund
    if is_historical_order and order_status in ('Shipped', 'Deliverd') and shipping_id is not None:
        order_status = 'Deliverd'
        
        # Ensure Day_of_arrival is set if it's currently NULL
        cursor.execute("SELECT Day_of_arrival, Shipping_date FROM Shipping WHERE Shipping_id = %s", (shipping_id,))
        result = cursor.fetchone()
        db_day_of_arrival = result[0]
        shipping_date = result[1]
        
        if db_day_of_arrival is None:
            day_of_arrival = shipping_date + timedelta(days=random.randint(1, 14))
            cursor.execute(
                "UPDATE Shipping SET Day_of_arrival = %s WHERE Shipping_id = %s",
                (day_of_arrival, shipping_id)
            )

    # Orders Insertion and Price Calculation
    order_price = 0.00 
    
    cursor.execute(
        """
        INSERT INTO Orders (Order_status, Order_price, Order_date, User_id, Shipping_id, Payment_id, Return_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (order_status, order_price, order_date, user_id, shipping_id, payment_id, return_id)
    )
    order_id = cursor.lastrowid
    
    # Insert Order_Product
    num_products_in_order = random.randint(1, 5)
    total_order_price = 0.0
    products_for_order = random.sample(product_ids, num_products_in_order)
    
    for product_id in products_for_order:
        product_name, unit_price = product_details[product_id]
        quantity = random.randint(1, 3)
        line_item_price = quantity * unit_price
        total_order_price += line_item_price
        
        cursor.execute(
            """
            INSERT INTO Order_Product (Order_id, Product_id, Quantity, Unit_price, Product_name)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (order_id, product_id, quantity, unit_price, product_name)
        )

    # Update Order_price
    total_order_price += shipping_cost
    cursor.execute(
        "UPDATE Orders SET Order_price = %s WHERE Order_id = %s",
        (round(total_order_price, 2), order_id)
    )
    
    if (i + 1) % 1000 == 0:
        conn.commit()
        print(f"  -> Progress: {i + 1}/{NUM_ORDERS} orders committed. Latest date: {order_date} (Status: {order_status})")

conn.commit()
print(f"  - Successfully inserted {NUM_ORDERS} final orders and related records.")


# ----------------------------------------------------
## 3️⃣ Phase 3: Insert Shopping Cart Data
# ----------------------------------------------------

print("\n--- Phase 3: Inserting Shopping Cart Data ---")

# --- A. Insert Carts that became Orders ---
# Fetch all User_id, Order_id, and Order_date pairs from the Orders table
cursor.execute("SELECT User_id, Order_id, Order_date FROM Orders")
orders_data = cursor.fetchall()
inserted_completed_carts = 0

for user_id, order_id, order_date in orders_data:
    order_placed = True
    
    # Cart creation date is randomly before or on the order date
    created_by_date = get_random_date(order_date - timedelta(days=7), order_date)
    
    cursor.execute(
        """
        INSERT INTO Shopping_cart (User_id, Order_id, Order_placed, Created_by)
        VALUES (%s, %s, %s, %s)
        """,
        (user_id, order_id, order_placed, created_by_date)
    )
    inserted_completed_carts += 1

print(f"  - Inserted {inserted_completed_carts} completed shopping carts.")

# --- B. Insert Abandoned Carts ---
NUM_ABANDONED_CARTS = 2000 
abandoned_carts_inserted = 0

for _ in range(NUM_ABANDONED_CARTS):
    user_id = random.choice(user_ids)
    
    # Abandoned carts have NULL Order_id and Order_placed = FALSE
    order_id = None
    order_placed = False
    
    # Created date is recent (last 90 days), but not today
    created_by_date = get_random_date(TODAY - timedelta(days=90), TODAY - timedelta(days=1))
    
    try:
        cursor.execute(
            """
            INSERT INTO Shopping_cart (User_id, Order_id, Order_placed, Created_by)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, order_id, order_placed, created_by_date)
        )
        abandoned_carts_inserted += 1
    except mysql.connector.IntegrityError:
        pass

conn.commit()
print(f"  - Inserted {abandoned_carts_inserted} abandoned shopping carts.")

# --- C. Final Data Consistency Check (Enforcing Business Rule) ---
# Update Order_placed = TRUE where Order_id is NOT NULL (just in case)
cursor.execute(
    """
    UPDATE Shopping_cart
    SET Order_placed = TRUE
    WHERE Order_id IS NOT NULL AND Order_placed = FALSE
    """
)
rows_updated = cursor.rowcount
conn.commit()
print(f"  - Final consistency check: Updated {rows_updated} rows to enforce: Order_id IS NOT NULL => Order_placed = TRUE.")

# ----------------------------------------------------
## 🛑 4. Close Connection
# ----------------------------------------------------

cursor.close()
conn.close()
print("\n🎉 Data insertion complete. Database connection closed.")