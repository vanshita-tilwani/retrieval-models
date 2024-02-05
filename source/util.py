from nltk.stem import PorterStemmer
import re

def sanitize(text):
    return text.strip().replace('\n', '')

def RemovePunctuation(text):
    punctuations = ['.', '`', ',', '"', '-', '(', ')', '\'']
    for p in punctuations:
        text = text.replace(p, ' ')
    return text

def RemoveNewLineCharacter(text) :
    text = text.replace("\n", ' ')
    return text

def StemSentences(text):
    stemmer = PorterStemmer()

    words = text.split(' ')
    for i in range(len(words)):
        words[i] = stemmer.stem(words[i])
    return ' '.join(words)

def RemoveStopwords(stopwords, text):
    text = text.lower()
    words = text.split(" ")
    for s in stopwords:
        while s in words:
            words.remove(s)
    text = ' '.join(words)
    
    return text

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
    return re.search(pattern, document).group().strip()