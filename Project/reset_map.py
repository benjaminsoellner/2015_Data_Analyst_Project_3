from pymongo import MongoClient
import os

def reset_map(jsonFile, mongoServer, mongoPort):
    os.remove(jsonFile)
    client = MongoClient(mongoServer + ":" + mongoPort)
    client.udacity.osmnodes.drop()