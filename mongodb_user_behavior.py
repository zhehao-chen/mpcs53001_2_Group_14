import mysql.connector
from pymongo import MongoClient
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

# --- 1. Configuration ---

# MySQL Configuration (Ensure this matches your database setup)
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Ad5021825!!',
    'database': 'eCommerce_DB'
}

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/" # Ensure MongoDB service is running
MONGO_DB_NAME = "ecommerce_behavior"
MONGO_COLLECTION_NAME = "user_events"

# Faker Initialization
fake = Faker()

# Simulation Parameters
NUM_EVENTS_PER_USER = 500 
EVENT_TYPES = ["product_view", "search_query", "click_product"]

# Global variables that will be populated dynamically from MySQL
PRODUCT_IDS = [] 
SEARCH_QUERIES = [] 

# =================================================================
# 2. MySQL Data Retrieval Functions
# =================================================================

def get_all_user_ids_from_mysql():
    """Retrieves all User_ids from the MySQL User table."""
    # (Function body remains the same as previous version)
    user_ids = []
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT User_id FROM User")
        results = cursor.fetchall()
        user_ids = [row[0] for row in results]
        print(f"✅ Successfully retrieved {len(user_ids)} User IDs from MySQL.")
    except mysql.connector.Error as err:
        print(f"❌ MySQL User Read Failed: {err}")
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    return user_ids

def get_all_product_ids_from_mysql():
    """Retrieves all Product_ids from the MySQL Product table."""
    # (Function body remains the same as previous version)
    product_ids = []
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT Product_id FROM Product")
        results = cursor.fetchall()
        product_ids = [row[0] for row in results]
        print(f"✅ Successfully retrieved {len(product_ids)} Product IDs from MySQL.")
    except mysql.connector.Error as err:
        print(f"❌ MySQL Product Read Failed: {err}")
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    return product_ids

def get_search_terms_from_mysql():
    """
    Retrieves product names from MySQL, cleans them, and creates a pool of realistic search terms.
    """
    search_terms_pool = set()
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Query product names and descriptions (if available, for richer terms)
        cursor.execute("SELECT Product_name FROM Product")
        product_names = cursor.fetchall()
        
        # Process each name
        for name_tuple in product_names:
            name = str(name_tuple[0]).lower()
            
            # Split by space, hyphen, etc., and remove single-character words
            # This generates terms like 'iphone', 'pro', 'max' from 'iPhone 15 Pro Max'
            words = [word.strip() for word in name.split() if len(word.strip()) > 1]
            
            # Add the full name and its components to the set
            search_terms_pool.add(name)
            for word in words:
                search_terms_pool.add(word)
                
        # Add a few common, non-product related terms for realism (e.g., brand names, or "sale")
        search_terms_pool.update(["sale", "clearance", "shipping", "guarantee"])
        
        print(f"✅ Created a pool of {len(search_terms_pool)} unique search terms.")
    except mysql.connector.Error as err:
        print(f"❌ MySQL Search Term Read Failed: {err}")
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    
    return list(search_terms_pool)

# =================================================================
# 3. Simulation & MongoDB Functions
# =================================================================

def generate_random_event(user_id, available_product_ids, available_search_queries):
    """Generates a single random user behavior event document."""
    
    if not available_product_ids or not available_search_queries:
        print("Error: Missing data for simulation.")
        return None
    
    event_time = fake.date_time_between(start_date='-30d', end_date='now')
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "timestamp": event_time,
        "event_type": event_type,
        "product_id": None,
        "search_query": None,
        "time_spent_ms": None,
    }

    if event_type in ["product_view", "click_product"]:
        event["product_id"] = random.choice(available_product_ids)
        if event_type == "product_view":
            event["time_spent_ms"] = random.randint(1000, 60000)
            
    elif event_type == "search_query":
        # Use the dynamically generated pool of search terms
        event["search_query"] = random.choice(available_search_queries)

    return event

def simulate_and_store_data(user_ids, product_ids, search_queries):
    """Connects to MongoDB, generates data, and performs a bulk insert."""
    if not user_ids or not product_ids or not search_queries:
        print("🛑 Critical data missing for simulation. Stopping.")
        return

    client = None
    try:
        # 1. Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        # 2. Bulk Generation of all events
        all_events = []
        for user_id in user_ids:
            for _ in range(NUM_EVENTS_PER_USER):
                # Pass the search queries list here
                event_data = generate_random_event(user_id, product_ids, search_queries)
                if event_data:
                    all_events.append(event_data)

        print(f"📝 Successfully generated a total of {len(all_events)} simulated events.")

        # 3. Bulk Write to MongoDB
        if all_events:
            collection.insert_many(all_events)
            print(f"✅ Successfully wrote {len(all_events)} event records to MongoDB collection: {MONGO_COLLECTION_NAME}")
            
            # 4. Create Index
            collection.create_index([("user_id", 1), ("timestamp", -1)])
            print("✅ Successfully created compound index (user_id, timestamp).")

    except Exception as e:
        print(f"❌ MongoDB or Data Processing Failed: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")


# =================================================================
# 4. Execute Main Program
# =================================================================

if __name__ == "__main__":
    # Step A: Retrieve all necessary data from MySQL
    try:
        user_ids_list = get_all_user_ids_from_mysql()
        
        # Populate the global lists dynamically
        PRODUCT_IDS = get_all_product_ids_from_mysql()
        SEARCH_QUERIES = get_search_terms_from_mysql()
        
    except Exception as e:
        print("\n--- PROGRAM HALTED ---")
        print("Error during MySQL data retrieval. Check your connection or table names.")
        exit()
    
    # Step B: Simulate and store data into MongoDB
    simulate_and_store_data(user_ids_list, PRODUCT_IDS, SEARCH_QUERIES)