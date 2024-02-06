from elasticsearch7 import Elasticsearch
from constants import Constants

# Indexing the documents in Elasticsearch
def indexDocuments(documents, stopwords) :
    if(doesIndexExist()):
        return
    createIndex(stopwords)
    for document in documents:
        addData(index,document, documents[document])
    print("Documents have been added to the index with name : {index}")

def doesIndexExist() :
    return es.indices.exists(index=index)

def getDocuments(scroll_size=1000):

    initial_query = {
        "query": {
            "match_all": {}
        },
        "size": scroll_size,
        "sort": ["_doc"]
    }
    response = es.search(index=index, body=initial_query, scroll="1m")
    documents = {hit["_id"]: hit["_source"]['content'] for hit in response["hits"]["hits"]}

    while len(response["hits"]["hits"]) > 0:
        scroll_id = response["_scroll_id"]
        response = es.scroll(scroll_id=scroll_id, scroll="1m")
        for hit in response["hits"]["hits"]:
            documents[hit["_id"]] = hit["_source"]['content']

    return documents

# Adding data in the Elasticsearch index
def addData(indexName, docID, text) :
    es.index(index=indexName, 
             document={
                 'content' : text
             }, id=docID)
    
# Create an Index in Elasticsearch
def createIndex(stopwords) :
    configurations = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": stopwords
                    },
                },
                "analyzer": {
                    "stopped": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "porter_stem"
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
    print("Index with name : {index} has been created in Elastic Search")



es = Elasticsearch("http://localhost:9200")
index = Constants.INDEX_NAME
