#-------------------------------------------------------------------------
# AUTHOR: Renard Pascual
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Create, Update, and Delete document as well as outputting inverted index ordered by term
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/
# Importing some Python libraries
from pymongo import MongoClient
# Create a database connection object using pymongo
def connectDataBase():
    # Create a database connection object using pymongo
    client = MongoClient('mongodb://localhost:27017/')
    db = client['documents_db']
    return db

# Function to create document
def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # Create a dictionary (document) to count how many times each term appears in the document.
    terms = docText.lower().split()
    term_count = {}
    for term in terms:
        if term in term_count:
            term_count[term] += 1
        else:
            term_count[term] = 1
# Use space " " as the delimiter character for terms and remember to lowercase them.
# Create a list of dictionaries (documents) with each entry including a term,
# its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    doc = {
            '_id': docId,
            'text': docText,
            'title': docTitle,
            'date': docDate,
            'category': docCat,
            'terms': term_count
        }
    # Insert the document
    col.insert_one(doc)
# Function to delete a document
def deleteDocument(col, docId):
    # Delete the document from the database
    col.delete_one({'_id': docId})

# Function to update a document
def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)
    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

# Function to get the inverted index
def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count
    index = {}
    for doc in col.find():
        for term, count in doc['terms'].items():
            if term in index:
                index[term] += ', ' + doc['title'] + ':' + str(count)
            else:
                index[term] = doc['title'] + ':' + str(count)
    # Sort the index by term
    sorted_index = dict(sorted(index.items()))
    return sorted_index
