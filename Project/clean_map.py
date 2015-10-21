from pymongo import MongoClient
from bson.objectid import ObjectId
from audit_quality_map import audit_phone_numbers
import pymongo
import pandas
import re
import pprint

phone_regex = re.compile(ur'^(\(?([\+|\*]|00) *(?P<country>[1-9][0-9]*)\)?)?' +  # country code
                         ur'[ \/\-\.]*\(?0?\)?[ \/\-\.]*' +  # separator
                         ur'(\(0?(?P<area1>[1-9][0-9 ]*)\)|0?(?P<area2>[1-9][0-9]*))?' +  # area code
                         ur'[ \/\-\.]*' +  # separator
                         ur'(?P<number>([0-9]+ *[\/\-.]? *)*)$', # number
                         re.UNICODE)

cuisines_synonyms = {
  'german': ['regional', 'schnitzel', 'buschenschank'],
  'portuguese': ['Portugiesisches_Restaurant_&_Weinbar'],
  'italian': ['pizza', 'pasta'],
  'mediterranean': ['fish', 'seafood'],
  'japanese': ['sushi'],
  'turkish': ['kebab'],
  'american': ['steak_house']
}
# not mapped:
# greek, asian, chinese, indian, international, vietnamese, thai, spanish, arabic
# sudanese, russian, korean, hungarian, syrian, vegan, soup, croatian, african
# balkan, mexican, french, cuban, lebanese
 

def clean_cuisines(db, debug, csvFilePattern, csvEncoding):
    if debug:
        print "Cleaning cuisines not possible in debug mode."
        return
    else:
        print "Creating new parameter cuisineTags..."
        db.eval('''
                db.osmnodes.find({
                        "cuisine": {"$exists": true},
                        "amenity": "restaurant"
                    }).snapshot().forEach(function(val, idx) {
                        val.cuisineTags = val.cuisine.split(';');
                        db.osmnodes.save(val)
                    })
            ''')
        print "Normalizing parameter cuisineTags..."
        for target in cuisines_synonyms:
            print "... for " + target + " using " + str(cuisines_synonyms[target])
            db.osmnodes.update(
                    {
                        "cuisine": {"$exists": True},
                        "amenity": "restaurant",
                        "cuisineTags": {"$in": cuisines_synonyms[target]}
                    },
                    {
                        "$pullAll": { "cusineTags": cuisines_synonyms[target] },
                        "$addToSet": { "cuisineTags": [ target ] }
                    },
                    multi=False
                )
        print "Done."


def clean_phones_parse(db):
    records = audit_phone_numbers(db.osmnodes)
    pos = pandas.DataFrame(columns = [ 'phone' , 'country' , 'area' , 'number', 'normalized' ])
    neg = pandas.DataFrame(columns = [ 'normalized' ])
    for record in records:
        m = phone_regex.search(unicode(record["phone"]))
        if m is not None:
            g = m.groupdict()
            country = ""
            normalized = ""
            area = ""
            number = ""
            if "country" in g.keys() and g["country"] is not None:
                country = g["country"]
                normalized += "+" + country
            if "area1" in g.keys() and g["area1"] is not None:
                area = g["area1"]
                normalized += (" " if normalized != "" else "0") + area
            elif "area2" in g.keys() and g["area2"] is not None:
                area = g["area2"]
                normalized += (" " if normalized != "" else "0") + area
            if "number" in g.keys() and g["number"] is not None:
                number = g["number"]
                normalized += (" " if normalized != "" else "") + number
            s = pandas.Series(data = [ unicode(record["phone"]) , country , area , number, normalized ],
                        index = [ 'phone' , 'country' , 'area' , 'number', 'normalized' ],
                        name = record["_id"])
            pos = pos.append(s)
        else:
            s = pandas.Series(data = [ unicode(record["phone"]) ],
                        index = [ 'normalized' ],
                        name = record["_id"])
            neg = neg.append(s)
    return (pos, neg)

def clean_phones(db, debug, csvFilePattern, csvEncoding):
    (pos, neg) = clean_phones_parse(db)
    if debug:
        csvFile = csvFilePattern.format("clean_phones.csv")
        print "Debug mode. Writing parsed phone names to file " + csvFile + "..."
        pos.to_csv(csvFile, encoding=csvEncoding)
        print
        print "These are the phone names we could parse:"
        pprint.pprint(pos)
        print
        print "The following objects couldn't be parsed:"
        pprint.pprint(neg)
        print
        print "We would touch these objects:"
        for index, record in pandas.concat([pos, neg]).iterrows():
            pprint.pprint(db.osmnodes.find_one({"_id": index}))
    else:
        print "Writing cleaned phone numbers to database."
        for index, record in pandas.concat([pos, neg]).iterrows():
            db.osmnodes.update({"_id": index}, {"phone": record["normalized"]}, multi=False)
        print "The following objects couldn't be parsed:"
        pprint.pprint(neg)
        

def clean_map(mongoServer, mongoPort, csvFilePattern, csvEncoding, debug):
    client = MongoClient(mongoServer + ":" + mongoPort)
    db = client.udacity
    #c = client.udacity.osmnodes
    print
    print "Cleaning cuisines."
    clean_cuisines(db, debug, csvFilePattern, csvEncoding)
    print "Done cleaning cuisines."
    #print
    #print "Cleaning phone numbers."
    #clean_phones(db, debug, csvFilePattern, csvEncoding)
    #print
    #print "Done cleaning phone numbers."
    