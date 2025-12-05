import mysql.connector
import pymongo
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# ---------------------------
# --- 1. Configuration ---
# ---------------------------

# MySQL Configuration (Modify according to your environment)
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Ad5021825!!"
MYSQL_DB_NAME = "eCommerce_DB"

# MongoDB Configuration (Modify according to your environment)
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "eCommerce_DB"
MONGO_COLLECTION_NAME = "product_details"

# ---------------------------
# --- 2. Helper Functions ---
# ---------------------------

def generate_mongo_product_document(mysql_id, category_id, category_name):
    """
    Generates a complex product detail document.
    Uses the MySQL Product_id and Category Name to generate contextually relevant fake data, 
    including custom attributes for 'Fashion'.
    """
    
    details = {
        "mysql_product_id": mysql_id, # <--- Key linking field
        "category_name": category_name, 
        "category_id": category_id,
        "description": fake.paragraph(nb_sentences=5),
        # Simulate rich content with HTML formatting
        "long_html_content": f"<p>{fake.text(max_nb_chars=500)}</p><h2>Features</h2><ul><li>High Quality</li><li>Durable Design</li><li>Easy to Use</li></ul>",
        "tags": random.sample(['luxury', 'portable', 'smart', 'eco', 'durable', 'vintage', 'new'], k=random.randint(1, 4)),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "review_count": random.randint(0, 500),
        "created_at": fake.past_date(start_date="-1y").isoformat() 
    }
    
    # Generate specifications based on category name
    if category_name == 'Electronics':
        details["technical_specs"] = {
            "weight_g": random.randint(100, 5000),
            "battery_life_hrs": random.randint(2, 24),
            "processor": random.choice(["Intel i7", "AMD Ryzen 9", "Apple M3"]),
            "warranty_years": random.randint(1, 5)
        }
    elif category_name == 'Books':
        details["book_info"] = {
            "author": fake.name(),
            "isbn": fake.isbn13(),
            "pages": random.randint(100, 1000),
            "publisher": fake.company(),
            "genre": random.choice(['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy'])
        }
    elif category_name == 'Home & Kitchen':
        details["materials"] = {
            "material": random.choice(['Stainless Steel', 'Ceramic', 'Glass', 'Cast Iron']),
            "color": fake.color_name(),
            "dishwasher_safe": random.choice([True, False])
        }
    elif category_name == 'Fashion':
        # --- Custom attributes for Fashion category: size, color, material ---
        available_sizes = random.sample(['XS', 'S', 'M', 'L', 'XL', 'XXL'], k=random.randint(2, 5))
        item_color = fake.color_name()
        primary_material = random.choice(["Cotton", "Polyester", "Wool", "Silk", "Leather", "Denim"])
        secondary_material = random.choice(["Spandex", "Nylon", "Rayon", "None"])
        
        # Build material composition description
        if secondary_material == "None" or random.random() < 0.5:
             material_composition = f"100% {primary_material}"
        else:
            material_composition = f"Primary: {primary_material}, Secondary: {secondary_material}"
            
        details["attributes"] = {
            "size": available_sizes,      # List of available sizes
            "color": item_color,          # Single color
            "material": material_composition # Material composition
        }
        details["brand"] = fake.company()
        details["care_instructions"] = random.choice(["Machine Wash Cold", "Dry Clean Only", "Hand Wash"])
        
    elif category_name == 'Toys':
        details["age_range"] = random.choice(['3+', '6+', '12+'])
        details["material"] = random.choice(['Plastic', 'Wood', 'Soft Fabric'])
    elif category_name == 'Sports':
         details["use_case"] = random.choice(['Running', 'Training', 'Outdoor', 'Team Sports'])
         details["water_resistant"] = random.choice([True, False])
    else: 
        details["misc"] = fake.word()
            
    return details

# ---------------------------
# --- 3. Main Logic ---
# ---------------------------

def sync_products_to_mongodb():
    
    # ----------------------------------------
    # Step A: Connect to MySQL and Fetch Data
    # ----------------------------------------
    mysql_conn = None
    # product_data structure: [(Product_id, Category_id, Category_name), ...]
    product_data = [] 
    
    try:
        mysql_conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB_NAME
        )
        cursor = mysql_conn.cursor()
        print(f"✅ Connected to MySQL database '{MYSQL_DB_NAME}'")

        # Use a JOIN query to fetch Product_id, Category_id, and Category Name
        query = """
        SELECT 
            p.Product_id, 
            p.Category_id,
            c.Name 
        FROM 
            Product p
        JOIN 
            Category c ON p.Category_id = c.Category_id
        ORDER BY 
            p.Product_id ASC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Extract the data list
        product_data = [(res[0], res[1], res[2]) for res in results]
        
        print(f"  - Successfully fetched {len(product_data)} product records with Category Names from MySQL.")

    except mysql.connector.Error as err:
        print(f"❌ Error connecting to or querying MySQL: {err}")
        return
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            print("  - MySQL connection closed.")

    if not product_data:
        print("⚠️ MySQL Product table is empty. Exiting.")
        return

    # ----------------------------------------
    # Step B: Connect to MongoDB and Insert Data
    # ----------------------------------------
    mongo_client = None
    
    try:
        mongo_client = pymongo.MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
        mongo_db = mongo_client[MONGO_DB_NAME]
        
        # Get the Collection
        product_details_collection = mongo_db[MONGO_COLLECTION_NAME] 
        
        # Clear the collection (to ensure a clean dataset for testing)
        product_details_collection.delete_many({}) 
        
        print(f"✅ Connected to MongoDB database '{MONGO_DB_NAME}'")

        # 3. Generate MongoDB Documents
        mongo_documents = []
        for product_id, category_id, category_name in product_data:
            # Pass all three fields to the generation function
            mongo_documents.append(generate_mongo_product_document(product_id, category_id, category_name))
            
        # 4. Bulk Insert Data
        if mongo_documents:
            product_details_collection.insert_many(mongo_documents)
            
            # 5. Create Index (Crucial for fast lookups)
            # Create a unique index on the linking field
            product_details_collection.create_index([("mysql_product_id", pymongo.ASCENDING)], unique=True)
            
            print(f"  - Successfully inserted {len(mongo_documents)} product detail documents into '{MONGO_COLLECTION_NAME}'.")
            print("  - Created unique index on 'mysql_product_id'.")

    except Exception as e:
        print(f"❌ Error connecting to or inserting into MongoDB: {e}")
    finally:
        if mongo_client:
            mongo_client.close()
            print("  - MongoDB connection closed.")

if __name__ == "__main__":
    print("--- Starting MongoDB Data Synchronization Script ---")
    # 
    sync_products_to_mongodb()
    print("--- Script Finished ---")