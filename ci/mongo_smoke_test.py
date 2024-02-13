from pymongo import MongoClient

if __name__ == "__main__":
    client = MongoClient(connectTimeoutMS=5000)
    db_list = client.list_database_names()
    print(db_list)
