import os
from pymongo import MongoClient
import dotenv

dotenv.load_dotenv()
connection_string = os.environ.get("CONNECTION_STRING")

def get_database()-> MongoClient | None:
    try:
        print("Connecting to MongoDB...")
        db = MongoClient(connection_string)
    except Exception as e:
        print(e)
        return None

    print("Connected to MongoDB")
    return db
