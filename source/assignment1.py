from fileio import fetchQueries, fetchStopwords, fetchDocuments, OutputToFile, DeleteResultFiles
from indexer import indexDocuments, doesIndexExist, getDocuments
from esclient import ExecuteQuery

isIndexAvailable = doesIndexExist()
if(isIndexAvailable):
    documents = getDocuments()
else:
    documents = fetchDocuments()

stopwords = fetchStopwords()
indexDocuments(documents=documents, stopwords=stopwords)

queries = fetchQueries()

models = ['okapitf', 'tfidf', 'bm25', 'unigramlm_laplace', 'unigramlm_jelinekmercer']
DeleteResultFiles('esbuiltin')
for model in models:
    DeleteResultFiles(model)

for query in queries :
    esbuiltInScores = ExecuteQuery('esbuiltin', query=queries[query], documents=documents)
    for idx, hit in enumerate(esbuiltInScores['hits']['hits']):
        OutputToFile('esbuiltin', query, hit['_id'], idx+1, hit['_score'])

    for model in models:
        modelScore = ExecuteQuery(model, query=queries[query], documents=documents)
        for index, (document, score)in enumerate(modelScore):
            OutputToFile(model, query, document, index+1, score)
    print(f'All models executed for Query with ID : {str(query)}')        
