import os
import re
from elasticsearch7 import Elasticsearch
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict


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

def ES_search(query) :
    return es.search(index=index, query={'match' : {'content' : query}}, size=1000)

def OkapiTF(query) :
    scores= OrderedDict()
    averageLength = word_count['total']/len(documents)
    for document in documents:
        score = 0
        # TODO : check if this works
        length = word_count[document]
        for word in query.split(" "):
            if(word in term_vectors[document]):
                tf = term_vectors[document][word]['term_freq']
                denominator = tf + 0.5 + 1.5 * (length/averageLength)
                currentScore = tf/denominator
                score+= currentScore
        scores[document] = score
    return scores


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


#Utils

def fetch_term_vectors(document):
    body = {
        'ids': [document],
        'parameters': {
            'fields': ['content'],
            'field_statistics': True,
            'term_statistics': True
        }
    }
    term_vector = es.mtermvectors(index=index, body=body)
    if 'content' in term_vector['docs'][0]['term_vectors']:
        return document, term_vector['docs'][0]['term_vectors']['content']['terms']
    else:
        return document, {}
    
def fetchTermVectors() :
    term_vectors = {}
    with ThreadPoolExecutor() as executor:
        # Submit tasks to fetch term vectors for each document
        futures = {executor.submit(fetch_term_vectors, document): document for document in documents}
        # Retrieve results
        for future in futures:
            document, term_vector = future.result()
            term_vectors[document] = term_vector
    return term_vectors
    
def analyze_text(document_text):
    try:
        tokens = es.indices.analyze(body={"text": document_text, "analyzer": "standard"})
        return len(tokens['tokens'])
    except Exception as e:
        print(f"Error analyzing text: {e}")
        return 0
    
def fetchWordCount() :
    word_count = {}
    total_word_count = 0
    docs = es.mget(index=index, body={"ids": list(documents.keys())})
    with ThreadPoolExecutor() as executor:
        # Submit tasks to analyze text for each document
        futures = {executor.submit(analyze_text, doc['_source']['content']): doc['_id'] for doc in docs['docs']}
        for future in futures:
            document_id = futures[future]
            try:
                word_count[document_id] = future.result()
                total_word_count += word_count[document_id]
            except Exception as e:
                print(f"Error processing document {document_id}: {e}")
    word_count['total'] = total_word_count
    return word_count
# Main Program 

# Elastic Search Client and Index Name
es = Elasticsearch("http://localhost:9200")
index ="ap89_data1"
documents = fetchDocuments()
#createIndex()
#print("Index with name : {index} has been created in Elastic Search")
#indexDocuments()
#print("All the documents have been added to the elasticsearch index named {index}")

queries = fetchQueries()


# ES Built In
for query in queries :
    esbuiltResponse = ES_search(queries[query])
        
    with open(f'/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/trec_eval/esbuiltin_results.txt', 'a') as output_file:
        for idx, hit in enumerate(esbuiltResponse['hits']['hits']):
            docno = hit['_id']
            score = hit['_score']
            output_file.write(f"{query} Q0 {docno} {idx+1} {score} Exp\n")

term_vectors  = fetchTermVectors()
word_count = fetchWordCount()

# Okapi TF
for query in queries :
    scores = OkapiTF(queries[query])
    sortedScores = OrderedDict(sorted(scores.items(), key=lambda x: x[1]))
    okapiTfResponse = list(sortedScores.items())[::-1][:1000]
    with open(f'/Users/vanshitatilwani/Documents/Courses/CS6200/hw1-vanshita-tilwani/trec_eval/okapitf_results.txt', 'a') as output_file:
        for index, (document, score)in enumerate(okapiTfResponse):
            output_file.write(f"{query} Q0 {document} {index+1} {score} Exp\n")

    print('Hi')