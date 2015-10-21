import urllib
import zipfile
import pandas
import pprint
from pymongo import MongoClient

def zipcode_download(url, compressedFile, tempFolder):
    # the filename where the zipcodes are stored
    zipFilePath = tempFolder+"/zipcodes.zip.tmp"
    filePath = tempFolder
    # retrieve the zip file of zipcodes
    urllib.urlretrieve(url, zipFilePath)
    # unpack compressed file
    with zipfile.ZipFile(zipFilePath, 'r') as z:
        z.extract(compressedFile, filePath)
    return filePath + "/" + compressedFile

def zipcode_import(filePath):
    return pandas.read_csv(filePath, sep='\t', header=None,
                    names=["country", "zipcode", "place", "order1", "code1", "order2", "code2", "order3", "code3", "lat", "lon", "accuracy"],
                    converters={"zipcode": (lambda x: x)}
                )

def zipcode_write(zipcodeDataframe, mongoServer, mongoPort):
    client = MongoClient(mongoServer + ":" + mongoPort)
    client.udacity.zipcodes.drop()
    for (i, z) in zipcodeDataframe.iterrows():
        client.udacity.zipcodes.insert(dict(z))
    return client.udacity.zipcodes

def zipcode_map(zipcodeUrl, zipcodeCompressedFile, zipcodeFileTempFolder, mongoServer, mongoPort):
    zipcodeFilePath = zipcode_download(zipcodeUrl, zipcodeCompressedFile, zipcodeFileTempFolder)
    print "Downloaded to " + zipcodeFilePath
    zipcodeDataframe = zipcode_import(zipcodeFilePath)
    print "Converted to Dataframe with " + str(len(zipcodeDataframe.index)) + " rows"
    zipcodeColl = zipcode_write(zipcodeDataframe, mongoServer, mongoPort)
    print "Creating index"
    zipcodeColl.create_index([("zipcode", pymongo.TEXT)])
    print "Wrote to MongoDB - here is an entry:"
    pprint.pprint(zipcodeColl.find_one())
    