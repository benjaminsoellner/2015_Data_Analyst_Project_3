## Note: you must install Levenshtein module
# pip install python-Levenshtein
# for this Microsoft Visual C++ 9.0 is required. Get it from http://aka.ms/vcpython27
# more info - see: http://stackoverflow.com/questions/18134437/where-can-the-documentation-for-python-levenshtein-be-found-online

from pymongo import MongoClient
import pymongo
import pprint
import bson
import pandas
import Levenshtein

EXPECTED_STREET_PATTERN = \
    u"^.*(?<![Ss]tra\u00dfe)(?<![Ww]eg)(?<![Aa]llee)(?<![Rr]ing)(?<![Bb]erg)" + \
    u"(?<![Pp]ark)(?<![Hh]\u00f6he)(?<![Pp]latz)(?<![Bb]r\u00fccke)(?<![Gg]rund)$"

def audit_streets(collection):
    return list(collection.distinct("name", {
                    "type": "way",
                    "name": {"$regex": EXPECTED_STREET_PATTERN}
                }))

def audit_buildings(db):
    result = db.eval('''
            db.osmnodes.ensureIndex({pos:"2dsphere"});
            result = [];
            db.osmnodes.find(
                    {"building": {"$exists": true}, "address.street": {"$exists": true}, "pos": {"$exists": true}},
                    {"address.street": "", "pos": ""}
                ).forEach(function(val, idx) {
                    val.nearby = db.osmnodes.distinct("address.street",
                            {"_id": {"$ne": val._id}, "pos": {"$near": {"$geometry": {"type": "Point", "coordinates": val.pos}, "$maxDistance": 50, "$minDistance": 0}}}
                        );
                    result.push(val);
                })
            return result;
        ''')
    df_list = []
    for row in result:
        street_name = row["address"]["street"]
        nb_best_dist = None
        nb_best = ""
        nb_worst_dist = None
        nb_worst = ""
        for nearby_street in row["nearby"]:
            d = Levenshtein.distance(nearby_street, street_name)
            if nb_best_dist == None or d < nb_best_dist:
                nb_best_dist = d
                nb_best = nearby_street
            if nb_worst_dist == None or d > nb_worst_dist:
                nb_worst_dist = d
                nb_worst = nearby_street
        df_list += [{
                "_id": row["_id"],
                "street_name": street_name,
                "num_nearby": len(row["nearby"]),
                "nb_best": nb_best,
                "nb_worst": nb_worst,
                "nb_best_dist": nb_best_dist,
                "nb_worst_dist": nb_worst_dist
            }]
    return pandas.DataFrame(df_list, columns=["_id", "street_name", "num_nearby", "nb_best", "nb_best_dist", "nb_worst", "nb_worst_dist"])

def audit_phone_numbers(collection):
    return list(collection.aggregate([
        {"$match": {"$or": [
            {"phone": {"$exists": True}},
            {"mobile_phone": {"$exists": True}},
            {"address.phone": {"$exists": True}}
          ]}},
        {"$project": {
            "_id": 1,
            "phone": {"$ifNull": ["$phone", {"$ifNull": ["$mobile_phone", "$address.phone"]}]}
          }}
      ]))
    
def audit_quality_map(mongoServer, mongoPort, csvFilePattern, csvEncoding):
    client = MongoClient(mongoServer + ":" + mongoPort)
    db = client.udacity
    c = client.udacity.osmnodes
    print
    print "Auditing way descriptions..."
    print "These are the 'unusual' street names"
    r = audit_streets(c)
    pprint.pprint(r)
    print
    print "Auditing streets close to buildings..."
    r = audit_buildings(db)
    r.to_csv(csvFilePattern.format("audit_buildings.csv"), encoding=csvEncoding)
    pprint.pprint(r)
    print
    print "Auditing phone numbers..."
    r = audit_phone_numbers(c)
    pprint.pprint(r)
    