import re
import os
from concurrent.futures import ThreadPoolExecutor
from constants import Constants
from util import RemovePunctuation, RemoveStopwords, StemSentences, ParseDocuments, ParseDocumentID, ParseDocumentText, RemoveNewLineCharacter


# Read all the documents from the directory
def fetchDocuments() : 
    allDocuments = {}
# Read all the files documents to be indexed from the mentioned directory
    for filename in os.listdir(Constants.DATA_PATH):
        with open(os.path.join(Constants.DATA_PATH, filename), 'rb') as f:
            content = f.read().decode("iso-8859-1")
            documents = ParseDocuments(content)
            with ThreadPoolExecutor() as executor:
                # Submit tasks to fetch term vectors for each document
                futures = {executor.submit(sanitizeDocument, document): document for document in documents}
                # Retrieve results
                for future in futures:
                    docID, docText = future.result()
                    allDocuments[docID] = docText
    return allDocuments

# Sanitize Document Text by removing punctuations, stopwords and stemming the words
def sanitizeDocument(document) :
    docID = ParseDocumentID(document)
    docText = RemovePunctuation(ParseDocumentText(document))
    docText = RemoveNewLineCharacter(docText)
    docText = RemoveStopwords(fetchStopwords(), docText)
    docText = StemSentences(docText)
    return docID, docText

# Read all the stop words from the stoplist.txt which will be used in preprocessing data
def fetchStopwords() :
    with open(Constants.STOPWORDS_PATH, 'r') as f:
        content = f.read()
        names = content.split("\n")
    return names

# Read all the queries from the directory
def fetchQueries():
    query_key_value = []
    queries = {}
    stopwords = fetchStopwords()
    try:
        f = open(Constants.QUERY_PATH, 'r').read()
        f = RemovePunctuation(f)
        f = f.split('\n')

        for query in f:
            query_key_value = re.split('\s{3}', query.strip())
            if len(query_key_value) == 2:
                query_key_value[1] = RemoveStopwords(stopwords, query_key_value[1])
                queries[query_key_value[0]] = query_key_value[1]

        for key in queries:
            queries[key] = StemSentences(queries[key])

        return queries

    except Exception as exception:
        print(exception)

#Outputs the data into file
def OutputToFile(model, query_no, doc_no, rank, score):
    try:
        out = open(Constants.RESULTS_PATH + model + '.txt', 'a')
        out.write(str(query_no) + ' Q0 ' + str(doc_no) + ' ' +
                str(rank) + ' ' + str(score) + ' Exp\n')
        out.close()
    except Exception as exception:
        print(exception)

def DeleteResultFiles(model) :
    file_path =  Constants.RESULTS_PATH+model+'.txt'
    if(os.path.exists(file_path)):
        os.remove(file_path)