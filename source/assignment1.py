import os
import re
from elasticsearch7 import Elasticsearch


# Region 1 - Text Pre - Processing i.e. fetching documents, stopwords and parsing the data

# Read all the documents from the directory
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

# Read all the stop words from the stoplist.txt which will be used in preprocessing data
def fetchStopwords() :
    stopWordFile = "/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/IR_data/IR_data/AP_DATA/stoplist.txt"
    with open(stopWordFile, 'r') as f:
        content = f.read()
        names = content.split("\n")
    return names

# Read all the queries from the directory
def fetchQueries() :
    allQueries = "/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/IR_data/IR_data/AP_DATA/query_desc.51-100.short.txt"
    queries = {}
    with open(allQueries, 'r') as f:
        content = f.readlines()
    for line in content:
        query_no = int(line.split(".")[0])
        query_text = line.split(".")[1].strip()
        queries[query_no] = query_text
    return queries

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

# Region 2 - Elastic Search - Creating Index in Elastic Search and adding documents to the index

# Create an Index in Elasticsearch
def createIndex() :
    configurations = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": fetchStopwords()
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
    es.indices.create(index=index, body=configurations)

# Indexing the documents in Elasticsearch
def indexDocuments() :
    for document in documents:
        addData(index,document, documents[document])

# Adding data in the Elasticsearch index
def addData(indexName, docID, text) :
    es.index(index=indexName, 
             document={
                 'content' : text
             }, id=docID)

# Region 3 - 

# Execute queries and retrieve results
def execute_queries(index):

    # Read queries from query file
    with open('/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/IR_data/IR_data/AP_DATA/query_desc.51-100.short.txt', 'r') as query_file:
        queries = query_file.readlines()

    for query in queries:
        # Parse the query and extract query number and text
        query_number, query_text = query.split(' ', 1)
        
        esbuiltResponse = ES_search(index, query_text)
        
        with open(f'esbuiltin_results_{query_number}.txt', 'w') as output_file:
            for idx, hit in enumerate(esbuiltResponse['hits']['hits']):
                docno = hit['_id']
                score = hit['_score']
                output_file.write(f"{query_number} Q0 {docno} {idx+1} {score} Exp\n")

        okapiTFResponse = OkapiTF(index, query_text)

        with open(f'okapitf_results_{query_number}.txt', 'w') as output_file:
            for idx, hit in enumerate(okapiTFResponse['hits']['hits']):
                docno = hit['_id']
                score = hit['_score']
                output_file.write(f"{query_number} Q0 {docno} {idx+1} {score} Exp\n")
        
        TfidfResponse = TFIDF(index, query_text)
        with open(f'tfidf_results_{query_number}.txt', 'w') as output_file:
            for idx, hit in enumerate(TfidfResponse['hits']['hits']):
                docno = hit['_id']
                score = hit['_score']
                output_file.write(f"{query_number} Q0 {docno} {idx+1} {score} Exp\n")

def fetchTermVectors() :
    term_vectors = {}
    for document in list(documents.keys()):
        body = {
            'ids' : [document],
            'parameters' : {
                'fields' : ['content'],
                'field_statistics' : True,
                'term_statistics' : True
            }
        }
        term_vectors[document] = es.mtermvectors(index= index, body=body)
    return term_vectors

def ES_search(indexName, query) :
    return es.search(index=indexName, query={'match' : {'content' : query}}, size=1000)

def OkapiTF(indexName, query) :
    score = 0
    term_vector = fetchTermVectors()
    for word in query.split(" ") :
        score += 1

def TFIDF(indexName, query):
    return es.search(index=indexName, body={
            "query": {
                "match": {"content": query}
            },
            "size": 1000,
            "script_fields": { 
                "tfidf_score": {
                    "script": {
                        "source": "doc['text'].tf() * Math.log(_index['text'].df + 1)"
                    }
                }
            },
            "_source": False
        })

# Main Program 

# Elastic Search Client and Index Name
es = Elasticsearch("http://localhost:9200")
index ="ap89_data0"
documents = fetchDocuments()
#createIndex()
#print("Index with name : {index} has been created in Elastic Search")
#indexDocuments()
#print("All the documents have been added to the elasticsearch index named {index}")
queries = fetchQueries()
# ES Built In
for query in queries :
    esbuiltResponse = ES_search(index, queries[query])
        
    with open(f'/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/results/esbuiltin_results.txt', 'a') as output_file:
        for idx, hit in enumerate(esbuiltResponse['hits']['hits']):
            docno = hit['_id']
            score = hit['_score']
            output_file.write(f"{query} Q0 {docno} {idx+1} {score} Exp\n")
