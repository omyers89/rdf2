import json
import urllib
from HTMLParser import HTMLParser
from lxml import etree
import re

import csv
DEBUG = True

name = r'([A-Z][a-z]+ )+'
year = r'[1-9][1-9][1-9][1-9]'


class TableParser(HTMLParser):
    def __init__(self, dataKeyword):
        HTMLParser.__init__(self)
        self.in_table = False

        self.in_data = False
        self.dataAttr = dataKeyword
        self.re_obj = re.compile(r"^(.*)" + dataKeyword + "(.*)$",re.I)

    def handle_starttag(self, tag, attrs):
        if tag == 'table' and ('class', 'infobox vcard') in attrs:
            self.in_table = True

    def handle_data(self, data):
        if self.in_table:
            if self.re_obj.match(data):
                self.in_data = True
        if self.in_data and data != "":
            if re.match(name, data):
                print "name is:", data
            if re.match(year, data):
                print "year is:", data

    def handle_endtag(self, tag):
        if self.in_data:
            if tag == 'table':
                self.in_table = False
                self.in_data = False

#api_key = open('../API_key.txt').read()
#api_key = 'AIzaSyAhokgs_ncsIxGetz64lcXqBun5cBamPVA'
# query = 'DONALD TRUMP'
# service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
# params = {
#     'query': query,
#     'limit': 1,
#     'indent': True,
#    # 'key': api_key,
# }

def example():
    url = "https://en.wikipedia.org/wiki/Zara_Bate"
    response = urllib.urlopen(url).read()
    # table = etree.HTML(response)
    # rows = iter(table)
    # #headers = [col.text for col in next(rows)]
    # for row in rows:
    #     values = [col.text for col in row]
    #     print values

    parser = TableParser('spouse')
    parser.feed(response)


if __name__ == '__main__':
    print 'work'
    violation_dict = {}
    with open('../results/politician_single_incs_csv.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        i = 0
        for row in spamreader:

            row = ', '.join(row).split(',')
            print row
            if len(row) != 2 or ('\\' in row[0]):
                    continue
            violation_dict[tuple(row)] = {}
            violation_dict[tuple(row)]['subjet_title'] = get_subj_from_uri(row[0])
            violation_dict[tuple(row)]['property'] = get_property_from_uri(row[1])
            violation_dict[tuple(row)]['related_objects'] = get_related_objects_from_uri(row)

            i+=1
            if i > 15 and DEBUG:
                break



#print response
