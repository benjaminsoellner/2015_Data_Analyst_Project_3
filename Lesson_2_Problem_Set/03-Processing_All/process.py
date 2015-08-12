#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Let's assume that you combined the code from the previous 2 exercises
# with code from the lesson on how to build requests, and downloaded all the data locally.
# The files are in a directory "data", named after the carrier and airport:
# "{}-{}.html".format(carrier, airport), for example "FL-ATL.html".
# The table with flight info has a table class="dataTDRight".
# There are couple of helper functions to deal with the data files.
# Please do not change them for grading purposes.
# All your changes should be in the 'process_file' function
from bs4 import BeautifulSoup
from zipfile import ZipFile
import os

datadir = "data"


def open_zip(datadir):
    with ZipFile('{0}.zip'.format(datadir), 'r') as myzip:
        myzip.extractall()


def process_all(datadir):
    files = os.listdir(datadir)
    return files


def process_file(f):
    # This is example of the datastructure you should return
    # Each item in the list should be a dictionary containing all the relevant data
    # Note - year, month, and the flight data should be integers
    # You should skip the rows that contain the TOTAL data for a year
    # data = [{"courier": "FL",
    #         "airport": "ATL",
    #         "year": 2012,
    #         "month": 12,
    #         "flights": {"domestic": 100,
    #                     "international": 100}
    #         },
    #         {"courier": "..."}
    # ]
    data = []
    info = {}
    info["courier"], info["airport"] = f[:6].split("-")
    
    with open("{}/{}".format(datadir, f), "r") as html:
        soup = BeautifulSoup(html)
        flights_by_month = {}
        for table in soup.find_all(name="table"):
            if table.get('class') != None and table.get('class')[0] == "dataTDRight":
                for tr in table.find_all(name="tr"):
                    tds = tr.find_all(name="td")
                    if ("Year" not in tds[0].text) and ("TOTAL" not in tds[0].text) and ("TOTAL" not in tds[1].text):
                        year = int(tds[0].text)
                        month = int(tds[1].text)
                        domestic = int(tds[2].text.replace(",",""))
                        international = int(tds[3].text.replace(",",""))
                        if (year not in flights_by_month):
                            flights_by_month[year] = {}
                        if (month not in flights_by_month[year]):
                            flights_by_month[year][month] = (0,0)
                        flights_by_month[year][month] = ( domestic, international )

    for year in flights_by_month:
        for month in flights_by_month[year]:
            (domestic, international) = flights_by_month[year][month]
            data.append({
                    "courier": info["courier"],
                    "airport": info["airport"],
                    "year": int(year),
                    "month": int(month),
                    "flights": {"domestic": domestic, "international": international}
                })
        

    return data


def test():
    print "Running a simple test..."
    #open_zip(datadir)
    files = process_all(datadir)
    data = []
    for f in files:
        data += process_file(f)
    assert len(data) == 3
    for entry in data[:3]:
        assert type(entry["year"]) == int
        assert type(entry["flights"]["domestic"]) == int
        assert len(entry["airport"]) == 3
        assert len(entry["courier"]) == 2
    print "... success!"

if __name__ == "__main__":
    test()