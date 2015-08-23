from autos import process_file


def insert_autos(infile, db):
    autos = process_file(infile)
    db.autos.insert(data)
  
if __name__ == "__main__":
    
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples

    insert_autos('autos-small.csv', db)
    print db.autos.find_one()