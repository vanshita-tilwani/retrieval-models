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

DeleteResultFiles('esbuiltin')
DeleteResultFiles('okapitf')
# Okapi TF
for query in queries :
    esbuiltInScores = ExecuteQuery('esbuiltin', query=queries[query], documents=documents)
    for idx, hit in enumerate(esbuiltInScores['hits']['hits']):
        OutputToFile('esbuiltin', query, hit['_id'], idx+1, hit['_score'])
        print(f'Esbuilt executed for Query with ID : {str(query)}')
    
    okapitfScores = ExecuteQuery('okapitf', query=queries[query], documents=documents)
    for index, (document, score)in enumerate(okapitfScores):
            OutputToFile('okapitf', query, document, index+1, score)
            print(f'OkapiTF executed for Query with ID : {str(query)}')
                    
