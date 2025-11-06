"""
Check MongoDB Atlas for stored Kafka messages
"""
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Atlas configuration
MONGO_URI = os.getenv('MONGO_ATLAS_URI')
if not MONGO_URI:
    raise ValueError("MONGO_ATLAS_URI not found in environment or .env file. Please set your MongoDB Atlas connection string.")

print("Connecting to MongoDB Atlas...\n")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client['kafka_data']
collection = db['probando_messages']

# Get statistics
total_docs = collection.count_documents({})
print("=" * 60)
print("MongoDB Statistics")
print("=" * 60)
print(f"Database: kafka_data")
print(f"Collection: probando_messages")
print(f"Total documents: {total_docs:,}")
print()

if total_docs > 0:
    # Get first and last document
    first_doc = collection.find_one(sort=[('inserted_at', 1)])
    last_doc = collection.find_one(sort=[('inserted_at', -1)])
    
    print(f"First document inserted: {first_doc.get('inserted_at', 'N/A')}")
    print(f"Last document inserted: {last_doc.get('inserted_at', 'N/A')}")
    print()
    
    # Show 3 sample documents
    print("Sample Documents (latest 3):")
    print("-" * 60)
    for i, doc in enumerate(collection.find().sort('inserted_at', -1).limit(3), 1):
        print(f"\n[{i}] Document ID: {doc.get('_id', 'N/A')}")
        kafka_metadata = doc.get('kafka_metadata', {})
        offset = kafka_metadata.get('offset', 'N/A') if isinstance(kafka_metadata, dict) else 'N/A'
        print(f"    Kafka Offset: {offset}")
        print(f"    Data: {doc.get('data', 'N/A')}")
        print(f"    Inserted: {doc.get('inserted_at', 'N/A')}")
else:
    print("No documents found yet. Make sure the consumer is running!")

client.close()
