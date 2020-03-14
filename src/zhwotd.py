'''
Created on Mar 12, 2020

@author: kirk@kirkkittell.com
'''

import mysql.connector
import configparser
from builtins import None


# GENERAL DATABASE FUNCTIONS
def connect_to_db():
    # TODO: probably a better way to do this config reading than reading it every. single. time.
    config = configparser.ConfigParser()
    config.read("../config.ini")
    
    # TODO: abstract database settings out to config file
    conn = mysql.connector.connect(user=config["DATABASE"]["user"], 
                               password=config["DATABASE"]["password"], 
                               host=config["DATABASE"]["host"], 
                               database=config["DATABASE"]["database"])
    return conn


def query_db(q, q_tuple=None):
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)
    
    if q_tuple is None:
        cursor.execute(q)
    else:
        cursor.execute(q, q_tuple)
    
    results = list()
    for result in cursor:
        results.append(result)
        
    cursor.close()
    conn.close()
    return results
    

# SPECIFIC QUERIES

def select_wotd(d=None, w=None):
    # Build query
    if d is not None:
        q = ("SELECT date, word FROM wotd WHERE date = %s")
        q_tuple = (d.strftime('%Y-%m-%d'),)      # tuple with one value must have a comma...
    elif w is not None:
        q = ("SELECT date, word FROM wotd WHERE word = %s")
        q_tuple = (w,)
    
    result = query_db(q, q_tuple)
    
    return result

def select_term(term):
    # Build query
    q = ("SELECT * FROM dictionary WHERE term = %s")
    q_tuple = (term,)      # tuple with one value must have a comma...
    result = query_db(q, q_tuple)
    
    return result

# WOTD

class WOTD:
    def __init__(self, d=None, word=None):
        self.d = d
        self.word = word
        
    def is_in_database(self):
        result = False
        
        if self.word is None:
            print("Term missing from WOTD entry")
        else:
            q_result = select_wotd(w=self.word)
        
        if len(q_result) > 0:
            result = True
            
        return result

class WOTD_DB:
    def __init__(self):
        self.name = "wotd"
        
    def count_term_in_wotd_db(self, term):
        q_result = select_wotd(w=term.term)
        result = len(q_result)
        return result
    

# DICTIONARY

class Dictionary_DB:
    def __init__(self):
        self.name = "dictionary"
    
    def is_term_in_dictionary(self, term):
        q_result = select_term(term.term)
        result = False
        if len(q_result) > 0:
            result = True
        return result
        
    def add_terms_to_dictionary(self, terms):
        # TODO: show details in console about what was added and not added
        
        # Look for terms missing from dictionary, and add them
        for term in terms:
            if self.is_term_in_dictionary(term) == True:
                print("Not added: {} is already in dictionary".format(term.term))
            else:
                print("Added: {}".format(term.term))

class Term:
    def __init__(self, term_dict):
        # Required
        # TODO: stop operation if this is empty
        self.term = term_dict['term']
        
        # Sort of optional -- important, but don't stop operation for now
        self.pinyin = None
        self.definition = None
        if 'pinyin' in term_dict:
            self.pinyin = term_dict['pinyin']
        if 'definition' in term_dict:
            self.definition = term_dict['definition']
        
        # Optional
        self.traditional = None
        self.hsk = None
        if 'traditional' in term_dict:
            self.traditional = term_dict['traditional']
        if 'hsk' in term_dict:
            self.hsk = term_dict['hsk']

# DASHBOARD
wotd_db = WOTD_DB()
dict_db = Dictionary_DB()

# TEST
    
def test_function():
    t = Term(term="能力")
    result = wotd_db.count_term_in_wotd_db(t)
    
    x = "is not in database"
    if result > 0:
        x = "is in database"
    print("{} {}".format(t.term, x))
    return 

def test_function2():
    t = Term(term="能力")
    result = dict_db.is_term_in_dictionary(t)
    
    x = "is not in database"
    if result > 0:
        x = "is in database"
    print("{} {}".format(t.term, x))
    return 

test_function()
test_function2()