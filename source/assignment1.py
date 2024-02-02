
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

# Parse all the documents from the given content
def parseDocuments(content: str) :
    pattern = '(?s)(?<=<DOC>)(.*?)(?=</DOC>)'
    return re.findall(pattern, content)

# Parse the document ID from the document
def parseDocumentID(document: str) :
    pattern = '(?s)(?<=<DOCNO>)(.*?)(?=</DOCNO>)'
    return re.search(pattern, document).group().strip()

# Parse the document text from the document
def parseDocumentText(document: str) :
    pattern = '(?s)(?<=<TEXT>)(.*?)(?=</TEXT>)'
    return re.search(pattern, document).group().strip()

allDocuments = {}
# Read all the files documents to be indexed from the mentioned directory
allFiles = "/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/IR_data/IR_data/AP_DATA/ap89_collection"
for filename in os.listdir(allFiles):
    with open(os.path.join(allFiles, filename), 'rb') as f:
        content = f.read().decode("iso-8859-1")
        documents = parseDocuments(content)
        for document in documents:
            docID = parseDocumentID(document)
            docText = parseDocumentText(document)
            allDocuments[docID] = docText
print('Done parsing documents')

es = Elasticsearch("http://localhost:9200")
print(es.ping())