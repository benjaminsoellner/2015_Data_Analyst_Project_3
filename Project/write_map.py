from pymongo import MongoClient
import pymongo
import json

def write_map(jsonFile, mongoServer, mongoPort):
    client = MongoClient(mongoServer + ":" + mongoPort)
    client.udacity.osmnodes.drop()
    collection = client.udacity.osmnodes
    counter = 0
    print "Writing documents..."
    with open(jsonFile, "r") as f:
        for line in f:
            try:
                collection.insert(json.loads(line))
            except KeyboardInterrupt:
                raise
            except:
                print
                print "Error storing: " + line
            counter += 1
            if counter % 100000 == 0:
                print ".",
        print
    print "Creating index: name ..."
    collection.create_index([("name", pymongo.TEXT)])
    print "Creating index: pos ..."
    collection.create_index([("pos", pymongo.GEOSPHERE)])
    print "Creating index: id ..."
    collection.create_index([("id", pymongo.ASCENDING)])
    return counter