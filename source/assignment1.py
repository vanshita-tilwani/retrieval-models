
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

# Create an Index in Elasticsearch
def createIndex(indexName) :
    configurations = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": getStopwords()
                    }
                },
                "analyzer": {
                    "stopped": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "stopped",
                    "index_options": "positions"
                }
            }
        }
    }
    es.indices.create(index=indexName, body=configurations)

def addData(indexName, docID, text) :
    es.index(index=indexName, 
             document={
                 'content' : text
             }, id=docID)

def ES_Search(indexName, query) :
    return es.search(index=indexName, query={'match' : {'content' : query}}, size=1000)

def fetchDocuments() : 
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
    return allDocuments

def indexDocuments(index_name) :
    documents = fetchDocuments()
    for document in documents:
        addData(index_name,document, documents[document])
# Main Program

es = Elasticsearch("http://localhost:9200")
index ="ap89_data0"
createIndex(index)
indexDocuments(index)
print("Documents have been added to the index")