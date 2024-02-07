from fileio import fetchQueries, fetchStopwords, fetchDocuments, WriteToResults, DeleteResultFiles, OutputToFile, FetchHighRankedDocumentsForEachQuery
from indexer import indexDocuments, doesIndexExist, getDocuments
from esclient import ExecuteQuery, getMostDistinctiveTerms, init, getSignificantTerms
from constants import Constants

# Pre-processing documents and indexing them to elastic search
isIndexAvailable = doesIndexExist()
if(isIndexAvailable):
    documents = getDocuments()
else:
    documents = fetchDocuments()

stopwords = fetchStopwords()
indexDocuments(documents=documents, stopwords=stopwords)

# Fetching Data from elastic search which will be used to run retrieval models
init(documents=documents)

# Reading all the queries from the file
queries = fetchQueries()

# Cleaning up the result directory
models = [Constants.OKAPI_TF, Constants.TF_IDF, Constants.BM_25, Constants.LM_LAPLACE, Constants.LM_JELINEKMERCER]

DeleteResultFiles(Constants.ES_BUILT_IN)
for model in models:
    DeleteResultFiles(model)

# Running query for each retrieval model
for query in queries :
    esbuiltInScores = ExecuteQuery(Constants.ES_BUILT_IN, query=queries[query], documents=documents)
    for idx, hit in enumerate(esbuiltInScores['hits']['hits']):
        OutputToFile(Constants.ES_BUILT_IN, query, hit['_id'], idx+1, hit['_score'])

    for model in models:
        score = ExecuteQuery(model, query=queries[query], documents=documents)
        WriteToResults(model, query, score)
    print(f'All models executed for Query with ID : {str(query)}')

# Pseudo-Relevance using General Algorithm and ES Significant Terms API

DeleteResultFiles(Constants.RELEVANCE_MODEL+'_with_general_relevancy')
DeleteResultFiles(Constants.RELEVANCE_MODEL+'_with_esfeedback')

topDocuments = FetchHighRankedDocumentsForEachQuery()
most_distinct_terms = getMostDistinctiveTerms(topDocuments)
significant_terms = getSignificantTerms(queries, stopwords)
for query in queries :
    updatedQuery_General = queries[query] +' '+ most_distinct_terms[query]
    general_score = ExecuteQuery(Constants.RELEVANCE_MODEL, query=updatedQuery_General, documents=documents)
    WriteToResults(Constants.RELEVANCE_MODEL+"_with_general_relevancy", query, general_score)
    print(f'{Constants.RELEVANCE_MODEL} executed with General Algorithm for Relevance Feedback for the following updated query  : "{str(updatedQuery_General)}"')
    
    updatedQuery_ES = queries[query] +' '+ significant_terms[query]
    es_score = ExecuteQuery(Constants.RELEVANCE_MODEL, query=updatedQuery_ES, documents=documents)
    WriteToResults(Constants.RELEVANCE_MODEL+"_with_esfeedback", query, es_score)
    print(f'{Constants.RELEVANCE_MODEL} executed with Relevance Feedback using ES Significant Terms for the following updated query  : "{str(updatedQuery_ES)}"')