import mysql.connector
import redis
import uuid
import time
from datetime import datetime
import random 

# --- Configuration ---
# MySQL Settings (MUST BE CORRECT for the script to run)
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Ad5021825!!',
    'database': 'eCommerce_DB'
}

# Redis Settings
REDIS_CLIENT = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
# --- Constants ---
SESSION_EXPIRY_SECONDS = 3600  # 1 hour
VIEWED_LIST_MAX_SIZE = 20

print("--- Starting Full E-commerce Management Script ---")

# =================================================================
# 1. MySQL Data Retrieval Functions
# =================================================================

def get_user_id_by_email(email):
    """
    Connects to MySQL and retrieves the SINGLE User_id based on a given email address.
    """
    mysql_conn = None
    user_id = None

    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()
        print(f"✅ Connected to MySQL database 'eCommerce_DB'")

        query = "SELECT User_id FROM User WHERE Email = %s"
        cursor.execute(query, (email,))
        
        result = cursor.fetchone() 

        if result:
            user_id = result[0]
            print(f"✅ Found unique User_id {user_id} for email: {email}")
        else:
            print(f"⚠️ User not found for email: {email}")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error during user lookup: {err}")
        # Raising error to halt execution if DB connection is truly broken
        raise 
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            
    return user_id

def get_product_price_from_mysql(product_id: int):
    """
    Retrieves the Price for a specific product from the Product table.
    Assumes the Product table has a 'Price' column.
    """
    mysql_conn = None
    price = None

    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()
        
        query = "SELECT Unit_price FROM Product WHERE Product_id = %s"
        cursor.execute(query, (product_id,))
        
        result = cursor.fetchone()

        if result:
            # Convert the Decimal type from MySQL to float
            price = float(result[0])
            print(f"✅ Retrieved unit price for P{product_id}: ${price:.2f}")
        else:
            print(f"⚠️ Price not found for P{product_id} in Product table.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error retrieving price: {err}")
        raise
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            
    return price

# =================================================================
# 2. Inventory and Order Processing Functions
# =================================================================

def initialize_redis_stock_from_mysql(product_id):
    """Reads Quantity from MySQL Inventory and sets a persistent counter in Redis."""
    redis_key = f"stock:{product_id}"
    mysql_conn = None
    stock_quantity = 0

    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()
        
        query = "SELECT Quantity FROM Inventory WHERE Product_id = %s"
        cursor.execute(query, (product_id,))
        
        result = cursor.fetchone()

        if result:
            stock_quantity = int(result[0])
            REDIS_CLIENT.set(redis_key, stock_quantity) 
            print(f"✅ Initialized Redis stock for P{product_id}: {stock_quantity}")
        else:
            print(f"⚠️ Product P{product_id} not found in MySQL Inventory.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error during initialization: {err}")
        raise
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            
    return stock_quantity

def update_mysql_inventory(product_id, quantity_change):
    """Updates the Quantity field in the MySQL Inventory table."""
    mysql_conn = None
    
    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()

        query = "UPDATE Inventory SET Quantity = Quantity + %s WHERE Product_id = %s"
        cursor.execute(query, (quantity_change, product_id))
        mysql_conn.commit()
        print(f"✅ MySQL Inventory updated for P{product_id}. Change: {quantity_change}")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Update Error: {err}")
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()

def process_order_and_reduce_redis_stock(order_items: dict):
    """Atomically reduces stock in Redis."""
    print("\n--- Processing Order Stock Reduction in Redis ---")
    
    pipeline = REDIS_CLIENT.pipeline()
    stock_keys = [f"stock:{pid}" for pid in order_items.keys()]
    
    pipeline.watch(*stock_keys) 

    # Fetch current stocks using the pipeline for efficiency
    # Note: pipeline.get() returns the *pipeline object* itself in multi/exec block, 
    # but when executed outside multi(), it fetches the value immediately. 
    # Here we are relying on pipeline.get() behavior before pipeline.multi()
    current_stocks = {}
    
    # We must execute the WATCH commands *before* fetching values if we want them guaranteed fresh
    # However, since we're using pipeline.get() outside of the transaction block, we must manually 
    # fetch the watched values *before* multi().
    
    # Simple, non-pipelined fetch for stock check after WATCH
    stocks_result = REDIS_CLIENT.mget(stock_keys)
    
    for i, product_id in enumerate(order_items.keys()):
        # stocks_result returns bytes/None, convert to int
        stock_value = stocks_result[i]
        current_stocks[product_id] = int(stock_value or 0) if stock_value else 0

    for product_id, quantity in order_items.items():
        current_stock = current_stocks.get(product_id, 0)
        if current_stock < quantity:
            REDIS_CLIENT.unwatch() # Use client method for unwatch
            print(f"❌ FAILED: Insufficient stock for P{product_id}. Available: {current_stock}, Requested: {quantity}")
            return False 

    try:
        pipeline.multi() 
        for product_id, quantity in order_items.items():
            stock_key = f"stock:{product_id}"
            pipeline.decrby(stock_key, quantity) 
            
        pipeline.execute() 
        print("✅ Redis stock successfully reduced atomically.")
        return True
        
    except redis.WatchError:
        print("❌ FAILED: Redis transaction aborted due to concurrent modification.")
        return False
    finally:
        pipeline.reset()

def create_mysql_order_record(user_id: int, cart_id: int, items_with_price: dict, total_cost: float):
    """
    Writes a new payment, order, and updates the shopping cart record to placed 
    in MySQL tables in a single transaction.
    `items_with_price` format: {product_id: (quantity, unit_price)}
    """
    mysql_conn = None
    order_id = None
    
    # 1. Simulation Choices for Payment
    PAYMENT_METHODS = ['Credit_card', 'Debt_card', 'PayPal']
    PAYMENT_STATUSES = ['Paid', 'Unpaid']
    
    simulated_payment_method = random.choice(PAYMENT_METHODS)
    simulated_payment_status = random.choice(PAYMENT_STATUSES)

    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()
        
        # Start transaction for atomicity (Payment, Order, and Cart Update)
        mysql_conn.start_transaction()

        # STEP A: Insert into Payment Table
        print(f"DEBUG: Creating Payment record (Method: {simulated_payment_method}, Status: {simulated_payment_status})")
        payment_query = """
        INSERT INTO Payment (Payment_method, Payment_status)
        VALUES (%s, %s)
        """
        cursor.execute(payment_query, (simulated_payment_method, simulated_payment_status))
        payment_id = cursor.lastrowid
        
        # STEP B: Insert into Orders Table
        order_query = """
        INSERT INTO Orders (Order_status, Order_price, Order_date, User_id, Shipping_id, Payment_id, Return_id)
        VALUES (%s, %s, CURDATE(), %s, %s, %s, %s)
        """
        # Set required default/NULL values based on your schema
        order_status = 'Placed'
        shipping_id = None
        return_id = None
        
        cursor.execute(order_query, (
            order_status,
            total_cost,
            user_id,
            shipping_id,
            payment_id,   
            return_id     
        ))
        order_id = cursor.lastrowid

        # STEP C: Update Shopping_cart Table (Link Order and Mark as Placed)
        cart_update_query = """
        UPDATE Shopping_cart SET Order_placed = TRUE, Order_id = %s
        WHERE Cart_id = %s AND User_id = %s
        """
        cursor.execute(cart_update_query, (order_id, cart_id, user_id))
        
        mysql_conn.commit() 
        print(f"✅ MySQL Order successfully created (Order ID: {order_id}, Payment ID: {payment_id})")
        print(f"✅ Shopping_cart (ID: {cart_id}) updated to Order_placed=TRUE and linked to Order_id: {order_id}")
        
    except mysql.connector.Error as err:
        print(f"❌ MySQL Order Creation FAILED: {err}. Attempting rollback.")
        if mysql_conn:
            mysql_conn.rollback() 
        order_id = None
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            
    return order_id


# =================================================================
# 3. Cart Management (MySQL & Redis)
# =================================================================

def get_or_create_shopping_cart_id(user_id: int):
    """
    Checks for an unplaced Shopping_cart for the user. If none exists, creates a new one.
    Returns the Cart_id.
    """
    mysql_conn = None
    cart_id = None
    
    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = mysql_conn.cursor()
        
        # 1. Check for an existing, unplaced cart (Order_placed = FALSE)
        check_query = """
        SELECT Cart_id FROM Shopping_cart 
        WHERE User_id = %s AND Order_placed = FALSE
        LIMIT 1
        """
        cursor.execute(check_query, (user_id,))
        result = cursor.fetchone()
        
        if result:
            cart_id = result[0]
            print(f"✅ Found existing, unplaced Cart_id {cart_id} for User_id {user_id}.")
        else:
            # 2. No unplaced cart found, create a new one
            create_query = """
            INSERT INTO Shopping_cart (User_id, Created_by)
            VALUES (%s, CURDATE())
            """
            cursor.execute(create_query, (user_id,))
            mysql_conn.commit()
            cart_id = cursor.lastrowid
            print(f"✅ Created new Cart_id {cart_id} for User_id {user_id}.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error in cart ID management: {err}")
        if mysql_conn:
            mysql_conn.rollback()
        raise
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            
    return cart_id

def add_to_cart(user_id, product_id, quantity=1):
    """Adds or updates product quantity in the cart hash (Redis is the transient cart)."""
    cart_key = f"transient_cart:{user_id}" 
    REDIS_CLIENT.hincrby(cart_key, product_id, quantity)
    print(f"2. CART: Added {quantity}x of product {product_id} to **transient** cart {cart_key}.")

def remove_redis_cart_data(user_id: int):
    """Removes the transient cart hash from Redis after the order is successfully placed."""
    cart_key = f"transient_cart:{user_id}"
    deleted_count = REDIS_CLIENT.delete(cart_key)
    if deleted_count > 0:
        print(f"✅ Redis transient cart '{cart_key}' successfully removed.")
    else:
        print(f"⚠️ Redis transient cart '{cart_key}' not found for removal.")

# =================================================================
# 4. Redis Session Management (Auxiliary Functions)
# =================================================================

def start_user_session(user_id, device):
    """Creates a new session hash and sets an expiration time."""
    session_id = str(uuid.uuid4())
    session_key = f"session:{user_id}"
    
    REDIS_CLIENT.hset(session_key, mapping={
        'session_id': session_id,
        'user_id': user_id,
        'device': device,
        'login_time': datetime.now().isoformat()
    })
    
    REDIS_CLIENT.expire(session_key, SESSION_EXPIRY_SECONDS)
    print(f"\n1. SESSION: Started new session for user {user_id} on {device}.")

def view_product(user_id, product_id):
    """Adds a product to the viewed list, ensuring no duplicates and trimming the size."""
    viewed_key = f"viewed:{user_id}"
    REDIS_CLIENT.lrem(viewed_key, 1, product_id)
    REDIS_CLIENT.lpush(viewed_key, product_id)
    REDIS_CLIENT.ltrim(viewed_key, 0, VIEWED_LIST_MAX_SIZE - 1)
    print(f"3. VIEWED: User {user_id} viewed product {product_id}.")

# =================================================================
# 5. Simulation Execution
# =================================================================

# Sample Products for the demo
DEMO_PRODUCT = 1
DEMO_QUANTITY = 2

if __name__ == "__main__":
    
    # --- STEP 1: Get User ID, Product Price, and Cart ID from MySQL ---
    USER_EMAIL = 'martinkristen@example.com' 
    
    try:
        USER_ID = get_user_id_by_email(USER_EMAIL)
        DEMO_UNIT_PRICE = get_product_price_from_mysql(DEMO_PRODUCT)
        # Get or Create the MySQL Shopping_cart ID
        CART_ID = get_or_create_shopping_cart_id(USER_ID)
    except Exception as e:
        # Catch errors from the DB calls
        print(f"\n--- PROGRAM HALTED ---")
        print(f"Critical data retrieval failed. Error: {e}")
        exit()

    # Check if all critical IDs are present
    if USER_ID is not None and DEMO_UNIT_PRICE is not None and CART_ID is not None:
        print(f"\n--- Starting Redis Operations for User ID: {USER_ID} (Cart ID: {CART_ID}) ---")
        
        # --- A. Setup and Initialization ---
        # NOTE: Initialize stock only if it hasn't been set before in a real scenario
        initialize_redis_stock_from_mysql(DEMO_PRODUCT)
        start_user_session(USER_ID, "Desktop")
        add_to_cart(USER_ID, DEMO_PRODUCT, DEMO_QUANTITY)
        view_product(USER_ID, DEMO_PRODUCT)
        
        # --- B. Order Processing Preparation ---
        ORDER_PAYLOAD = {
            DEMO_PRODUCT: DEMO_QUANTITY
        }
        
        ITEMS_WITH_PRICE = {
            DEMO_PRODUCT: (DEMO_QUANTITY, DEMO_UNIT_PRICE)
        }
        
        SIMULATED_TOTAL_COST = DEMO_UNIT_PRICE * DEMO_QUANTITY
        
        print("\n--- ORDER PLACEMENT ATTEMPT ---")

        # Step B1 & B2: Atomic Redis Stock Reduction (PRE-CHECK)
        if process_order_and_reduce_redis_stock(ORDER_PAYLOAD):
            
            # Step B3: CRITICAL WRITE - Create transactional records in MySQL
            new_order_id = create_mysql_order_record(
                user_id=USER_ID, 
                cart_id=CART_ID,  # Passed the Cart_id here
                items_with_price=ITEMS_WITH_PRICE, 
                total_cost=SIMULATED_TOTAL_COST
            )

            if new_order_id:
                # Step B4: Update MySQL Inventory (Sync Write)
                for product_id, quantity in ORDER_PAYLOAD.items():
                    update_mysql_inventory(product_id, -quantity) 
                
                # Step B5: Remove the transient cart data from Redis
                remove_redis_cart_data(USER_ID)

                print("\n--- SYNCHRONIZATION SUMMARY ---")
                current_redis_stock = REDIS_CLIENT.get(f"stock:{DEMO_PRODUCT}")
                print(f"Order Completed! Final Redis Stock: {current_redis_stock}")
                print(f"Order was successfully logged under ID: {new_order_id}")
                
            else:
                print("\nORDER FAILED DUE to MySQL Order Creation Error. Stock is reduced, but Order record is missing! (Manual stock correction needed)")

        else:
            print("\nORDER FAILED DUE TO INVENTORY CONFLICT OR SHORTAGE. No changes made to databases.")

    else:
        print("\nCannot proceed with simulation. Missing USER_ID, PRODUCT_PRICE, or CART_ID.")