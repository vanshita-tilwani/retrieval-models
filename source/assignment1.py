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
print("All the documents have been added to the elasticsearch index named {index}")

queries = fetchQueries()

models = ['okapitf', 'tfidf', 'bm25']
DeleteResultFiles('esbuiltin')
for model in models:
    DeleteResultFiles(model)
# Okapi TF
for query in queries :
    esbuiltInScores = ExecuteQuery('esbuiltin', query=queries[query], documents=documents)
    for idx, hit in enumerate(esbuiltInScores['hits']['hits']):
        OutputToFile('esbuiltin', query, hit['_id'], idx+1, hit['_score'])
        print(f'ES_BUILTIN executed for Query with ID : {str(query)}')
    
    for model in models:
        modelScore = ExecuteQuery(model, query=queries[query], documents=documents)
        for index, (document, score)in enumerate(modelScore):
            OutputToFile(model, query, document, index+1, score)
            print(f'{model.upper()} executed for Query with ID : {str(query)}')

    print('Done')         
