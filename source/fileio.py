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
            with ThreadPoolExecutor(max_workers=16) as executor:
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
    docText = ParseDocumentText(document)
    docText = RemovePunctuation(docText)
    docText = RemoveNewLineCharacter(docText)
    words = docText.split(' ')
    words = RemoveStopwords(stopwords, words)
    words = StemSentences(words)
    return docID, ' '.join(words)

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
    try:
        f = open(Constants.QUERY_PATH, 'r').read()
        f = RemovePunctuation(f)
        f = f.split('\n')

        for query in f:
            query_key_value = re.split('\s{3}', query.strip())
            if len(query_key_value) == 2:
                words = query_key_value[1].split(' ')
                modifiedWords = RemoveStopwords(stopwords, words)
                modifiedWords = StemSentences(modifiedWords)
                queries[query_key_value[0]] = " ".join(set(modifiedWords))

        return queries

    except Exception as exception:
        print(exception)

# Fetching high ranked documents from the model retrieval result
def FetchHighRankedDocumentsForEachQuery():
    documents = {}
    for line in open(Constants.RESULTS_PATH +Constants.RELEVANCE_MODEL + '.txt', 'r'):
        [query, q0, document, rank, score, exp]= line.split(' ')
        if int(rank) == 1:
            documents[query] = [document]
    return documents

# Method to write the model retrieval output results to file
def WriteToResults(model, query, score_without_relevancy):
    relevant_documents = []
    try:
        out = open(Constants.RESULTS_PATH + model + '.txt', 'a')
        for rank, (document, score)in enumerate(score_without_relevancy):
            relevant_documents.append(document)
            out.write(str(query) + ' Q0 ' + str(document) + ' ' +str((rank+1)) + ' ' + str(score) + ' Exp\n')
        out.close()
        return relevant_documents
    except Exception as exception:
        print(exception)

# Method to write ES built in model output results to file
def OutputToFile(model, query_no, doc_no, rank, score):
    try:
        out = open(Constants.RESULTS_PATH + model + '.txt', 'a')
        out.write(str(query_no) + ' Q0 ' + str(doc_no) + ' ' +
                str(rank) + ' ' + str(score) + ' Exp\n')
        out.close()
    except Exception as exception:
        print(exception)

# Method to delete results file before each execution
def DeleteResultFiles(model) :
    file_path =  Constants.RESULTS_PATH+model+'.txt'
    if(os.path.exists(file_path)):
        os.remove(file_path)

stopwords = fetchStopwords()