#!/usr/bin/env python
# -*- coding: utf-8 -*-
# All your changes should be in the 'extract_airports' function
# It should return a list of airport codes, excluding any combinations like "All"

from bs4 import BeautifulSoup
html_page = "options.html"


def extract_airports(page):
    data = []
    with open(page, "r") as html:
        soup = BeautifulSoup(html)
        data = []
        for o in soup.find_all(id="AirportList")[0].find_all(name="option"):
            v = o.get('value')
            if v not in ['All', 'AllMajors', 'AllOthers']:
                data.append(v)

    return data


def test():
    data = extract_airports(html_page)
    assert len(data) == 15
    assert "ATL" in data
    assert "ABR" in data

test()