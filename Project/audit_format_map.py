from pymongo import MongoClient
import pymongo
import pandas
import re
import datetime
import pprint

def audit_structure_field(key, value, dataFrame):
    dataFrame.is_copy = False
    pprint
    if type(value) is dict:
        for subkey in value:
            dataFrame = audit_structure_field(key + ":" + subkey, value[subkey], dataFrame)
    else:
        if dataFrame.size == 0 or key not in dataFrame.index:
            dataFrame = dataFrame.append(pandas.Series(data={"float": 0, "int": 0, "str": 0, "other": 0},
                                                       name=key))
        if type(value) is float: 
            t = "float"
        elif type(value) is int:
            t = "int"
        elif type(value) is str or type(value) is unicode:
            t = "str"
        else:
            t = "other"
        dataFrame.loc[key,t] += 1
    return dataFrame

def audit_format_map(mongoServer, mongoPort, csvFile):
    client = MongoClient(mongoServer + ":" + mongoPort)
    cursor = client.udacity.osmnodes.find().batch_size(1000)
    dataFrame = pandas.DataFrame(columns=["float", "int", "str", "other"])
    print "Scanning documents ..."
    counter = 0
    for document in cursor:
        pprint.pprint(document)
        for k in document:
            if k in ["pos"]:
                pass
            else:
                dataFrame = audit_structure_field(k, document[k], dataFrame)
        counter += 1
        if counter % 1000 == 0:
            print str(counter) + " processed."
    print str(counter) + " processed."
    print "Done scanning. Writing the output to " + csvFile + "..."
    dataFrame.to_csv(csvFile, index=True, header=True)
    print "Done writing."