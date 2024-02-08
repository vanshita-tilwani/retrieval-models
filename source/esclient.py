from concurrent.futures import ThreadPoolExecutor
from elasticsearch7 import Elasticsearch
from constants import Constants
from collections import OrderedDict
import math
from collections import Counter

# Region Initialization

# Initializes vocab size, field vectors, term vectors and terms for entire corpus which
# will be used in models
def init(documents):
    setVocabSize()
    setFieldVectors(documents)
    setTermVectors(documents)
    setCorpusTermStatistics(documents)

# Executes query for given documents on model specified by type
def ExecuteQuery(type, query, documents) :
    modifiedQuery = query.split(' ')
    if(type == Constants.ES_BUILT_IN):
        return ES_search(query=modifiedQuery)
    else :
        result = executeModel(type, modifiedQuery, documents)
        orderedResult = OrderedDict(sorted(result.items(), key=lambda x: x[1]))
        scores = list(orderedResult.items())[::-1][:1000]
        return scores

# Executing the query based on model
def executeModel(type, query, documents) :
    scores= OrderedDict()
    for document in documents:
        score = 0
        for word in query:
            if(word in term_vectors[document]):
                score_wd = calculateScore(type, query, word, document)
            else :
                score_wd = calculateScoreForMissingTerm(type, document, word)
            score+= score_wd
        scores[document] = score
    return scores

# Calculates score for a term in a document based on specified model
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

# Calculates score for a term that is missing in a document based on specified model
def calculateScoreForMissingTerm(type, document, word) :
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
            return lm_jelinek_mercer_by_term_and_document_for_missing_terms(word, document)

# Psuedo - Relevance Feedback Helpers

# Method to fetch the most distinctive terms of a set of document (i.e. high TFIDF score)
def getMostDistinctiveTerms(documentsByQuery):
    terms = {}
    for query in documentsByQuery:
        tfidf_by_term = {}
        for document in documentsByQuery[query]:
            for term in term_vectors[document]:
                tfidf_by_term[term] = TFIDFScoreByTermInDocument(term, document)
        terms[query] = tfidf_by_term
    distinctive_terms = {} 
    for query in terms:
        index = Counter(terms[query])
        top = index.most_common(Constants.RELEVANCY_FEEDBACK_QUERY_EXP_COUNT)
        distinctive_terms[query] = ' '.join([i[0] for i in top])
    return distinctive_terms

# Method to fetch the most significant terms using ES API
def getSignificantTerms(queries, stopwords):
    significant_terms = {}
    for query in queries:
        significant_terms[query] = Counter()
        for word in queries[query].split(' '):
            body = {
                "query": {
                    "terms": {"content": [word]}
                },
                "aggregations": {
                    "significantCrimeTypes": {
                        "significant_terms": {
                            "field": "content"
                        }
                    }
                },
                "size": 0
            }
            response = es.search(index=Constants.INDEX_NAME, body=body)
            terms = response['aggregations']['significantCrimeTypes']['buckets']
            for term in terms:
                if term['key'] != word:
                    significant_terms[query][term['key']] += 1

    most_significant_terms = {}
    for query in significant_terms:
        top = significant_terms[query].most_common(Constants.ES_RELEVANCY_FEEDBACK_EXP_COUNT)
        most_significant_terms[query] = top
    return getHighTFIDFTermsInCorpus(most_significant_terms, stopwords)

# Private Helper Methods to evaluate score for each retrieval model
  
def TFIDFScoreByTermInDocument(term, document) :
    tf = term_vectors[document][term]['term_freq']
    d = field_statistics['doc_count']
    df = term_vectors[document][term]['doc_freq']
    score = math.log(1 + tf, 2) * math.log(d / df)
    return score

def TFIDFScoreByTermInCorpus(term):
    df = 100
    if term in term_statistics_in_corpus:
        df = term_statistics_in_corpus[term]['doc_freq']
    idf = math.log(field_statistics['doc_count']/df)
    return idf
  
def getHighTFIDFTermsInCorpus(most_significant_terms, stopwords):
    idf_scores = {}
    for query in most_significant_terms:
        terms_idf = {}
        for term in most_significant_terms[query]:
            if term not in stopwords:
                terms_idf[term] = TFIDFScoreByTermInCorpus(term)
        idf_scores[query] = terms_idf
        
    most_high_idf_terms = {} 
    for query in idf_scores:
        c = Counter(idf_scores[query])
        top = c.most_common(Constants.RELEVANCY_FEEDBACK_QUERY_EXP_COUNT)
        most_high_idf_terms[query] = ' '.join([i[0][0] for i in top])

    return most_high_idf_terms

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
    foreground = Constants.CORPUS_PROB * (tf/vocab_size)
    background = (1 - Constants.CORPUS_PROB) * ((ttf - tf)/(total_length - length))
    score = foreground + background
    return math.log(score)

def lm_jelinek_mercer_by_term_and_document_for_missing_terms(word, document):
    ttf = field_statistics['sum_ttf']/field_statistics['doc_count'] # keeping it as avg if not present in corpus todo : might cause issue
    length = getDocumentLength(term_vectors[document])
    total_length = field_statistics['sum_ttf']
    if(word in term_statistics_in_corpus):
            ttf = term_statistics_in_corpus[word]['ttf']
    score = (1-Constants.CORPUS_PROB)*(ttf/(total_length - length))
    return math.log(score)

def ES_search(query) :
    return es.search(index=index, query={'match' : {'content' : " ".join(query)}}, size=1000)

def getDocumentLength(term_vectors):
        doc_length = 0

        if len(term_vectors) == 0:
            return 0
        else:
            for term in term_vectors:
                doc_length += term_vectors[term]['term_freq']
            return doc_length

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
    with ThreadPoolExecutor() as executor:
        # Submit tasks to fetch term vectors for each document
        futures = {executor.submit(fetch_term_vectors, document): document for document in documents}
        # Retrieve results
        for future in futures:
            document, term_vector = future.result()
            term_vectors[document] = term_vector

def setCorpusTermStatistics(documents) :
    global term_statistics_in_corpus
    if(len(term_statistics_in_corpus) != 0):
        return
    for document in documents :
        for term in term_vectors[document]:
            if term not in term_statistics_in_corpus:
                term_statistics_in_corpus[term] = {"doc_freq": term_vectors[document][term]['doc_freq'], "ttf": term_vectors[document][term]['ttf']}

def setVocabSize() :
    global vocab_size
    if(vocab_size != 0):
        return
    vocab_size = fetch_unique_term_count()

es = Elasticsearch("http://localhost:9200")
index = Constants.INDEX_NAME
term_vectors = {}
term_statistics_in_corpus = {}
field_statistics = {}
vocab_size = 0