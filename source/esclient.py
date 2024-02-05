from concurrent.futures import ThreadPoolExecutor
from elasticsearch7 import Elasticsearch
from constants import Constants
from collections import OrderedDict
import math


def ExecuteQuery(type, query, documents) :
    modifiedQuery = query_analyzer(query=query)
    if(type == 'esbuiltin'):
        return ES_search(query=modifiedQuery)
    else :
        setFieldVectors(documents)
        setTermVectors(documents)
        
        
        if(type == 'okapitf'):
            result = OkapiTF(query=modifiedQuery, documents=documents)
            orderedResult = OrderedDict(sorted(result.items(), key=lambda x: x[1]))
            scores = list(orderedResult.items())[::-1][:1000]
            return scores
        
def ES_search(query) :
    return es.search(index=index, query={'match' : {'content' : " ".join(query)}}, size=1000)

def okapitf_wd(tf, length) :
    averageLength = field_statistics['sum_ttf']/field_statistics['doc_count']
    okapitf = denominator = tf + 0.5 + 1.5 * (length/averageLength) 
    return okapitf

def tfidf_wd(tf, df, length) :
    totalDocs = field_statistics['doc_count']
    okapitf_wd = okapitf_wd(tf, length)
    tfidf = okapitf_wd * math.log(totalDocs/df)
    return tfidf

def OkapiTF(query, documents) :
    scores= OrderedDict()
    for document in documents:
        okapitf = 0
        # TODO : check if this works
        length = getDocumentLength(term_vectors[document])
        for word in query:
            if(word in term_vectors[document]):
                tf = term_vectors[document][word]['term_freq']
                okapitf_wd = okapitf_wd(tf, length)
                okapitf+= okapitf_wd
        if(okapitf != 0.0):
            scores[document] = okapitf
    return scores

def TFIDF(query, documents) :
    scores= OrderedDict()
    
    for document in documents:
        tfidf = 0
        # TODO : check if this works
        length = getDocumentLength(term_vectors[document])
        for word in query:
            if(word in term_vectors[document]):
                tf = term_vectors[document][word]['term_freq']
                df = term_vectors[document][word]['doc_freq']
                tfidf_wd = tfidf_wd(tf, df, length)
                tfidf+= tfidf_wd
        if(tfidf != 0.0):
            scores[document] = tfidf
    return scores

def getDocumentLength(term_vectors):
        doc_length = 0

        if len(term_vectors) == 0:
            return 0
        else:
            for term in term_vectors:
                doc_length += term_vectors[term]['term_freq']
            return doc_length
        
def analyze_text(document_text):
    try:
        tokens = es.indices.analyze(body={"text": document_text, "analyzer": "standard"})
        return len(tokens['tokens'])
    except Exception as e:
        print(f"Error analyzing text: {e}")
        return 0
    
def query_analyzer(query) :
    body = {
        "tokenizer" : "standard",
        "filter" : ["porter_stem", "lowercase"],
        "text" : query
    }
    result = es.indices.analyze(body=body)
    return [list['token'] for list in result['tokens']]

def fetch_term_vectors(document):
    body = {
        'ids': [document],
        'parameters': {
            'fields': ['content'],
            'term_statistics': True
        }
    }
    term_vector = es.mtermvectors(index=index, body=body)
    # Setting Field Statistics
    if 'content' in term_vector['docs'][0]['term_vectors']:
        return document, term_vector['docs'][0]['term_vectors']['content']['terms']
    else:
        return document, {}

def fetch_field_statistics(document) :
    body = {
        'ids': [document],
        'parameters': {
            'fields': ['content'],
            'field_statistics': True,
        }
    }
    term_vector = es.mtermvectors(index=index, body=body)
    # Setting Field Statistics
    return term_vector['docs'][0]['term_vectors']['content']['field_statistics']
    
    
def setFieldVectors(documents) :
    global field_statistics
    if(len(field_statistics) != 0):
        return
    field_statistics = fetch_field_statistics(list(documents.keys())[0])

def setTermVectors(documents) :
    global term_vectors
    if(len(term_vectors) != 0):
        return
    with ThreadPoolExecutor() as executor:
        # Submit tasks to fetch term vectors for each document
        futures = {executor.submit(fetch_term_vectors, document): document for document in documents}
        # Retrieve results
        for future in futures:
            document, term_vector = future.result()
            term_vectors[document] = term_vector

def getDocumentLength(term_vectors):
        doc_length = 0

        if len(term_vectors) == 0:
            return 0
        else:
            for term in term_vectors:
                doc_length += term_vectors[term]['term_freq']
            return doc_length
        
es = Elasticsearch("http://localhost:9200")
index = Constants.INDEX_NAME
term_vectors = {}
field_statistics = {}