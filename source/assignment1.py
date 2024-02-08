from fileio import fetchQueries, fetchStopwords, fetchDocuments, WriteToResults, DeleteResultFiles, WriteToResults_ES, FetchHighRankedDocumentsForEachQuery
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
models = [Constants.ES_BUILT_IN, Constants.OKAPI_TF, Constants.TF_IDF, Constants.BM_25, Constants.LM_LAPLACE, Constants.LM_JELINEKMERCER]

for model in models:
    DeleteResultFiles(model)
    
# Running query for each retrieval model
for query in queries :
    for model in models:
        score = ExecuteQuery(model, query=queries[query], documents=documents)
        if(model == Constants.ES_BUILT_IN):
            WriteToResults_ES(model, query, score)
        else:
            WriteToResults(model, query, score)
    print(f'All models executed for Query with ID : {str(query)}')

# Pseudo-Relevance 

# Pseudo-relevance Feedback using General Algorithm
for model in models:
    DeleteResultFiles(model+'_with_general_relevancy')
    

for model in models :
    topDocuments = FetchHighRankedDocumentsForEachQuery(model)
    most_distinct_terms = getMostDistinctiveTerms(topDocuments)

    for query in queries :
        updatedQuery_General = queries[query] +' '+ most_distinct_terms[query]
        general_score = ExecuteQuery(model, query=updatedQuery_General, documents=documents)
        if(model == Constants.ES_BUILT_IN):
            WriteToResults_ES(model+'_with_general_relevancy', query, general_score)
        else:
            WriteToResults(model+"_with_general_relevancy", query, general_score)
        print(f'{model} executed with General Algorithm for Relevance Feedback for the following updated query  : "{str(updatedQuery_General)}"')
        
# Pseudo-relevance Feedback using Elastic Search "Significant Term" API

for model in models:
    DeleteResultFiles(model+'_with_esfeedback')

significant_terms = getSignificantTerms(queries, stopwords)
for query in queries :
    for model in models:
        updatedQuery_ES = queries[query] +' '+ significant_terms[query]
        es_score = ExecuteQuery(model, query=updatedQuery_ES, documents=documents)
        if(model == Constants.ES_BUILT_IN):
            WriteToResults_ES(model+'_with_esfeedback', query, es_score)
        else:
            WriteToResults(model+"_with_esfeedback", query, es_score)
        print(f'{model} executed with Relevance Feedback using ES Significant Terms for the following updated query  : "{str(updatedQuery_ES)}"')



