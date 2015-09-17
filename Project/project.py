import download_map
import process_map
import store_map
import write_map
import reset_map
import clean_map
import audit_format_map
import audit_stats_map
import audit_quality_map
import getopt
import sys

OSMURL="https://s3.amazonaws.com/metro-extracts.mapzen.com/dresden_germany.osm.bz2"
OSMFILE="data/dresden_germany.osm"
JSONFILE="tmp/dresden_germany.osm.json"
MONGOSERVER="localhost"
MONGOPORT="27017"
STRUCTURECSV="data/audit_format_map.csv"
IMAGEFILEPATTERN="data/{0}"
CSVFILEPATTERN="data/{0}"
ENCODING="iso_8859-1"

def run_download_map():
    print "Downloading map..."
    print "  from: ", OSMURL
    print "  to: ", OSMFILE
    download_map.download_map(OSMURL, OSMFILE)
    print "done."

def run_process_map():
    print "Processing map..."
    print "  from: ", OSMFILE
    print "  to: ", JSONFILE
    num = process_map.process_map(OSMFILE, JSONFILE)
    print "Processed " + str(num) + " second-level elements."

def run_write_map():
    print "Writing map..."
    print "  from: ", JSONFILE
    print "  to: ", (MONGOSERVER + ":" + MONGOPORT)
    num = write_map.write_map(JSONFILE, MONGOSERVER, MONGOPORT)
    print "Processed " + str(num) + " documents."

def run_audit_format_map():
    print "Analyzing map structure..."
    audit_format_map.audit_format_map(MONGOSERVER, MONGOPORT, STRUCTURECSV)

def run_audit_stats_map():
    print "Analyzing a few map statistics..."
    audit_stats_map.audit_stats_map(MONGOSERVER, MONGOPORT, IMAGEFILEPATTERN)

def run_audit_quality_map():
    print "Auditing data quality..."
    audit_quality_map.audit_quality_map(MONGOSERVER, MONGOPORT, CSVFILEPATTERN, ENCODING)

def run_reset_map():
    print "Resetting map..."
    print "  deleting JSON file: ", JSONFILE
    print "  dropping udacity.osmnodes collection from: ", (MONGOSERVER + ":" + MONGOPORT)
    reset_map.reset_map(JSONFILE, MONGOSERVER, MONGOPORT)

def run_clean_map(debug=False):
    clean_map.clean_map(MONGOSERVER, MONGOPORT, CSVFILEPATTERN, ENCODING, debug)

if __name__ == "__main__":
    optlist, _ = getopt.gnu_getopt(sys.argv, 'dpfswrqcC')
    options = [o[0] for o in optlist]
    if "-d" in options:
        run_download_map()
    if "-p" in options:
        run_process_map()
    if "-w" in options:
        run_write_map()
    if "-r" in options:
        run_reset_map()
    if "-f" in options:
        run_audit_format_map()
    if "-s" in options:
        run_audit_stats_map()
    if "-q" in options:
        run_audit_quality_map()
    if "-c" in options or "-C" in options:
        run_clean_map(debug=("-C" in options))
    if len(options) == 0:
        print "Usage: "
        print "  python project.py -d    Download & unpack bz2 file to OSM file (experimental)"
        print "  python project.py -p    Process OSM file and write JSON file"
        print "  python project.py -w    Write JSON file to MongoDB"
        print "  python project.py -f    Audit format / structure of data"
        print "  python project.py -s    Audit statistics of data"
        print "  python project.py -q    Audit quality of data"
        print "  python project.py -c    Clean data in MongoDB"
        print "  python project.py -C    Clean data debug mode - don't actually write to DB"