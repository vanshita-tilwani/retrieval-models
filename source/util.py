from nltk.stem import PorterStemmer
import re

def RemovePunctuation(text):
    punctuations = ['.', '`', ',', '"', '-', '(', ')', '\'', '$', ':', ';', '_', '\'']
    for p in punctuations:
        text = text.replace(p, ' ')
    return text

def RemoveNewLineCharacter(text) :
    text = text.replace("\n", ' ')
    return text

def StemSentences(words):
    for i in range(len(words)):
        words[i] = stemmer.stem(words[i])
    return words

def RemoveStopwords(stopwords, words):
    for s in stopwords:
        while s in words:
            words.remove(s)
    return words

# Parse all the documents from the given content
def ParseDocuments(content: str) :
    pattern = '(?s)(?<=<DOC>)(.*?)(?=</DOC>)'
    return re.findall(pattern, content)

# Parse the document ID from the document
def ParseDocumentID(document: str) :
    pattern = '(?s)(?<=<DOCNO>)(.*?)(?=</DOCNO>)'
    return re.search(pattern, document).group().strip()

# Parse the document text from the document
def ParseDocumentText(document: str) :
    pattern = '(?s)(?<=<TEXT>)(.*?)(?=</TEXT>)'
    return " ".join(re.findall(pattern, document))

stemmer = PorterStemmer()