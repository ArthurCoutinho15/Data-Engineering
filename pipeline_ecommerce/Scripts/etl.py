from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

class Etl():
    def __init__(self, uri, url, db_name, db_col) -> None:
        self.uri = uri
        self.url = url
        self.db_name = db_name
        self.db_col = db_col
        self.client = self.connect_mongo()
        self.db = self.create_connect_db()
        self.col = self.create_connect_collection()
        self.data = self.extract_api_data()
        
    
    def connect_mongo(self):
        client = MongoClient(self.uri, server_api=ServerApi('1'))
        try:
            client.admin.command('ping')
            print('Pinged your deployment. You sucessfully connected to MongoDB!')
        except Exception as e:
            print(e)
        return client
    
    def create_connect_db(self):
        db = self.client[self.db_name]
        
        return db
    
    def create_connect_collection(self):
        collection = self.db[self.db_col]
        
        return collection
       
    def extract_api_data(self):
        data = requests.get(self.url).json()
        return data
    
    def insert_data(col, data):
        result = col.insert_many(data)
        n_docs_inseridos = len(result.inserted_ids)
        
        return n_docs_inseridos
        
    if __name__ == ("__main__"):

        uri = "mongodb+srv://arthurtelescoutinho:!Dezembro15@cluster-pipeline.w4oto0g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline"
        client = connect_mongo(uri)
        db = create_connect_db(client, "db_produtos_desafio")
        col = create_connect_collection(db, 'produtos')

        data = extract_api_data('https://labdados.com/produtos')
        print(f"\nQuantidade de dados extraidos: {len(data)}")

        docs = insert_data(col, data)

        client.close()
    
         
            
        