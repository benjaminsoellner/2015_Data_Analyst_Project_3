import urllib
import bz2
import pprint

def download_map(osmUrl, osmFile):
    osmBz2File = osmFile+".bz2"
    print "Retrieving " + osmUrl + "..."
    urllib.urlretrieve(osmUrl, osmBz2File)
    print "Unpacking " + osmBz2File + " to " + osmFile + " ..."
    with open(osmFile, 'wb') as o, bz2.BZ2File(osmBz2File, 'rb') as i:
        for data in iter(lambda : i.read(100 * 1024), b''):
            o.write(data)
    print "Attention! There is a known issue!"
    print "  For some strange reason, this code sometimes only unpacks the first 900kB."
    print "  Please inspect the file if in doubt."
