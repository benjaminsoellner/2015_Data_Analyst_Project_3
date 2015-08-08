# -*- coding: utf-8 -*-
# Find the time and value of max load for each of the regions
# COAST, EAST, FAR_WEST, NORTH, NORTH_C, SOUTHERN, SOUTH_C, WEST
# and write the result out in a csv file, using pipe character | as the delimiter.
# An example output can be seen in the "example.csv" file.
import xlrd
import os
import csv
from zipfile import ZipFile
datafile = "2013_ERCOT_Hourly_Load_Data.xls"
outfile = "2013_Max_Loads.csv"


def open_zip(datafile):
    with ZipFile('{0}.zip'.format(datafile), 'r') as myzip:
        myzip.extractall()


def parse_file(datafile):
    workbook = xlrd.open_workbook(datafile)
    sheet = workbook.sheet_by_index(0)
    data = [["Station","Year","Month","Day","Hour","Max Load"]]
    stations = ["COAST", "EAST", "FAR_WEST", "NORTH", "NORTH_C", "SOUTHERN", "SOUTH_C", "WEST"]
    headers = {}
    for col in range(sheet.ncols):
        headers[sheet.cell_value(0, col)] = col
    for header in headers:
        if header in stations:
            station = header
            values = sheet.col_values(headers[station], start_rowx=1, end_rowx=sheet.nrows)
            maxvalue = max(values)
            maxpos   = values.index(maxvalue)
            maxtime  = xlrd.xldate_as_tuple( sheet.cell_value(maxpos+1, headers["Hour_End"]) , 0 )
            data = data + [[station,maxtime[0],maxtime[1],maxtime[2],maxtime[3],maxvalue]]
    return data

def save_file(data, filename):
    with open(filename, 'wb') as f:
        w = csv.writer(f, delimiter='|')
        for line in data:
            w.writerow(line)
    
def test():
    open_zip(datafile)
    data = parse_file(datafile)
    save_file(data, outfile)

    ans = {'FAR_WEST': {'Max Load': "2281.2722140000024", 'Year': "2013", "Month": "6", "Day": "26", "Hour": "17"}}
    
    fields = ["Year", "Month", "Day", "Hour", "Max Load"]
    with open(outfile) as of:
        csvfile = csv.DictReader(of, delimiter="|")
        for line in csvfile:
            s = line["Station"]
            if s == 'FAR_WEST':
                for field in fields:
                    assert ans[s][field] == line[field]

        
test()