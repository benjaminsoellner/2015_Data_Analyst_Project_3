from pymongo import MongoClient
import pymongo
import pprint
import pandas
import pprint

def audit_zipcode_list(nodeColl):
    pipeline = [
            { "$match": {"address.postcode": {"$exists": 1}} },
            { "$group": {"_id": "$address.postcode", "count": {"$sum": 1}} },
            { "$sort": {"count": 1} }
        ]
    return list(nodeColl.aggregate(pipeline))

def audit_zipcode_dict(zipcodeColl, zipcodeList):
    zipcodeArray = [z["_id"] for z in zipcodeList]
    zipcodeObjects = zipcodeColl.find( {"zipcode": {"$in": zipcodeArray}} )
    zipcodeDict = {z["zipcode"]: z["place"] for z in zipcodeObjects}
    return zipcodeDict

def audit_zipcode_join(zipcodeList, zipcodeDict):
    for z in zipcodeList:
        z["place"] = zipcodeDict[z["_id"]]
    return zipcodeList
    
def audit_zipcode_map(mongoServer, mongoPort, quiet=False):
    client = MongoClient(mongoServer + ":" + mongoPort)
    db = client.udacity
    n = client.udacity.osmnodes
    z = client.udacity.zipcodes
    if not quiet:
        print "Getting the list of zipcodes..."
    zipcodeList = audit_zipcode_list(n)
    if not quiet:
        print "Resolving them..."
    zipcodeDict = audit_zipcode_dict(z, zipcodeList)
    if not quiet:
        print "Mapping them..."
    zipcodeJoined = audit_zipcode_join(zipcodeList, zipcodeDict)
    if not quiet:
        pprint.pprint(zipcodeJoined)
    return zipcodeJoined
