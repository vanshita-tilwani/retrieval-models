
import sys
import os
import re
from elasticsearch7 import Elasticsearch

# Read all the stop words from the stoplist.txt which will be used in preprocessing data
def getStopwords() :
    stopWordFile = "/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/IR_data/IR_data/AP_DATA/stoplist.txt"
    with open(stopWordFile, 'r') as f:
        content = f.read()
        names = content.split("\n")
    return names