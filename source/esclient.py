from concurrent.futures import ThreadPoolExecutor
from elasticsearch7 import Elasticsearch
from constants import Constants
from collections import OrderedDict
import math
from collections import Counter

def init(documents):
    setVocabSize()
    setFieldVectors(documents)
    setTermVectors(documents)

def ExecuteQuery(type, query, documents) :
    modifiedQuery = query_analyzer(query=query)
    if(type == Constants.ES_BUILT_IN):
        return ES_search(query=modifiedQuery)
    else :
        result = executeModel(type, modifiedQuery, documents)
        orderedResult = OrderedDict(sorted(result.items(), key=lambda x: x[1]))
        scores = list(orderedResult.items())[::-1][:1000]
        return scores

def TFIDFScore(term, document) :
    tf = term_vectors[document][term]['term_freq']
    d = field_statistics['doc_count']
    df = term_vectors[document][term]['doc_freq']
    score = math.log(1 + tf, 2) * math.log(d / df)
    return score
  
def okapitf_by_term_and_document(word, document) :
    tf = term_vectors[document][word]['term_freq']
    length = getDocumentLength(term_vectors[document])
    averageLength = field_statistics['sum_ttf']/field_statistics['doc_count']
    denominator = tf + 0.5 + 1.5 * (length/averageLength) 
    okapitf = tf/denominator
    return okapitf

def tfidf_by_term_and_document(word, document) :
    totalDocs = field_statistics['doc_count']
    df = term_vectors[document][word]['doc_freq']
    okapitf_wd = okapitf_by_term_and_document(word, document)
    tfidf = okapitf_wd * math.log(totalDocs/df)
    return tfidf

def bm25_by_term_and_document(word, document, qf=1) :
    tf = term_vectors[document][word]['term_freq']
    df = term_vectors[document][word]['doc_freq']
    length = getDocumentLength(term_vectors[document])
    totalDocs = field_statistics['doc_count']
    averageLength = field_statistics['sum_ttf']/field_statistics['doc_count']
    firstTerm = math.log((totalDocs + 0.5)/(df+0.5))
    secondTerm = (tf + Constants.BM25_K1 * tf)/(tf + Constants.BM25_K1*((1-Constants.BM25_B) + Constants.BM25_B * length/averageLength))
    thirdTerm = (qf + Constants.BM25_K2*qf)/(Constants.BM25_K2 + qf)
    bm25 = firstTerm * secondTerm * thirdTerm
    return bm25

def lm_laplace_by_term_and_document(word, document) :
    length = getDocumentLength(term_vectors[document])
    tf = term_vectors[document][word]['term_freq']
    result = (tf + 1)/(length + vocab_size)
    return result

def lm_jelinek_mercer_by_term_and_document(word, document) :
    tf = term_vectors[document][word]['term_freq']
    ttf = term_vectors[document][word]['ttf']
    length = getDocumentLength(term_vectors[document])
    total_length = field_statistics['sum_ttf']
    foreground = Constants.CORPUS_PROB * (tf/length)
    background = (1 - Constants.CORPUS_PROB) * ((ttf - tf)/(total_length - length))
    score = foreground + background
    return math.log(score)

def ES_search(query) :
    return es.search(index=index, query={'match' : {'content' : " ".join(query)}}, size=1000)

def calculateScore(type, query, word, document) :
    match type:
        case Constants.OKAPI_TF :
            return okapitf_by_term_and_document(word, document)
        case Constants.TF_IDF :
            return tfidf_by_term_and_document(word, document)
        case Constants.BM_25 :
            return bm25_by_term_and_document(word, document, query.count(word))
        case Constants.LM_LAPLACE :
            return lm_laplace_by_term_and_document(word, document)
        case Constants.LM_JELINEKMERCER :
            return lm_jelinek_mercer_by_term_and_document(word, document)

def calculateScoreForMissingTerm(type) :
    match type:
        case Constants.OKAPI_TF :
            return 0.0
        case Constants.TF_IDF :
            return 0.0
        case Constants.BM_25 :
            return 0.0
        case Constants.LM_LAPLACE :
            return -1000.0
        case Constants.LM_JELINEKMERCER :
            return -1000.0
        
def executeModel(type, query, documents) :
    scores= OrderedDict()
    for document in documents:
        score = 0
        for word in query:
            if(word in term_vectors[document]):
                score_wd = calculateScore(type, query, word, document)
            else :
                score_wd = calculateScoreForMissingTerm(type)
            score+= score_wd
        scores[document] = score
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
        "filter" : [ "lowercase"],
        "text" : query
    }
    result = es.indices.analyze(body=body)
    return [list['token'] for list in result['tokens']]

def fetch_unique_term_count() :
    response = es.search(body= {
        "aggs": {
            "unique_term_count": {
            "cardinality": {
                "field": "content"
                }
            }
        }
    })
    return response['aggregations']['unique_term_count']['value']

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
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Submit tasks to fetch term vectors for each document
        futures = {executor.submit(fetch_term_vectors, document): document for document in documents}
        # Retrieve results
        for future in futures:
            document, term_vector = future.result()
            term_vectors[document] = term_vector

def setVocabSize() :
    global vocab_size
    if(vocab_size != 0):
        return
    vocab_size = fetch_unique_term_count()

def getDocumentLength(term_vectors):
        doc_length = 0

        if len(term_vectors) == 0:
            return 0
        else:
            for term in term_vectors:
                doc_length += term_vectors[term]['term_freq']
            return doc_length

def getMostDistinctiveTerms(documentsByQuery):
    terms = {}
    for query in documentsByQuery:
        tfidf_by_term = {}
        for document in documentsByQuery[query]:
            for term in term_vectors[document]:
                tfidf_by_term[term] = TFIDFScore(term, document)
        terms[query] = tfidf_by_term
    distinctive_terms = {} 
    for query in terms:
        index = Counter(terms[query])
        top = index.most_common(Constants.RELEVANCY_FEEDBACK_QUERY_EXP_COUNT)
        distinctive_terms[query] = ' '.join([i[0] for i in top])
    return distinctive_terms

es = Elasticsearch("http://localhost:9200")
index = Constants.INDEX_NAME
term_vectors = {}
field_statistics = {}
vocab_size = 0