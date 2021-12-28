'''
Created on Mar 12, 2020

@author: kirk@kirkkittell.com
'''

import mysql.connector
import configparser
from os.path import expanduser, join
import csv
import json
from datetime import datetime, date, timedelta
from dateutil import parser
from pinyinparser.parser import PinyinParser
from sqlalchemy import select

# GENERAL DATABASE FUNCTIONS
class DatabaseManager:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("../config.ini")
        db_file = self.config["FILES"]["db_file"]
        db_file_dir = self.config["FILES"]["input_dir"]
        self.db_file_fullpath = join(db_file_dir, db_file)
        self.db_json = json.load(self.db_file_fullpath)
    
    def connect_to_db(self):
        u = self.db_json['user']
        p = self.db_json['password']
        h = self.db_json['host']
        d = self.db_json['database']
        conn = mysql.connector.connect(user=u, password=p, host=h, database=d)
        return conn

    def query_db(self, q, q_tuple=None):
        conn = self.connect_to_db()
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
    

# WOTD

class WOTD:
    def __init__(self, d=None, word=None):
        self.d = d
        self.word = word
        
    def is_in_database(self):
        result = False
        z = ZhwotdQuery()
        
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

class InputFileParser:
    def __init__(self):
        pass
    
    def get_input_file_settings(self):
        input_dir = expanduser('~')
        input_dir = join(input_dir,'Dropbox','zhwotd')
        
        input_file_dictionary = 'zhwotd_input_dictionary.csv'
        input_filepath_dictionary = join(input_dir, input_file_dictionary)
        
        input_file_wotd = 'zhwotd_input_wotd.txt'
        input_filepath_wotd = join(input_dir, input_file_wotd)
        
        
    def parse_dictionary_input_csv(self, input_filepath):
        result = ''
        header = list()
        
        with open(input_filepath, newline='', encoding='utf8') as f:
            reader = csv.reader(f)
            col_pinyin = 0
            col_hsk = 0
            record_list = list()
            row_count = -1
            
            for row in reader:
                row_count += 1
                col_count = -1
                row_result = list()
                
                for col in row:
                    col_count += 1
                    
                    if row_count == 0:
                        # Get header information from first row
                        header.append(col)
                        
                        # Note which column contains pinyin entries
                        if col == 'pinyin':
                            col_pinyin = col_count
                        elif col == 'hsk':
                            col_hsk = col_count
                    
                    else:
                        if col_count == col_pinyin:
                            # Parse pinyin entry
                            col = parse_pinyin_entry(col)
                        elif col_count == col_hsk and col_count == '':
                            # if this entry is blank, set it to zero
                            col = '0'
    
                        row_result.append(col)
                        
                
                # Pack row_result into record_list
                if row_count > 0:
                    record_list.append(row_result)
        
        # After extracting info from file, create the SQL query
        result = pack_sql_insert('dictionary', header, record_list)
                    
        return result
    
    
    def parse_wotd_input(self, input_filepath):
        # Input is a list of words, one per line
        # No dates given--ask user for first date in the series
        
        # Build attribute list
        attribute_list = ['date', 'word']
        
        start_date_str = input('Start date:')
        # TODO: if input blank, use today
        # TODO: if input blank, query the database, use the next blank day
        if start_date_str.lower() == 'today':
            start_date = date.today()
        elif start_date_str.lower() == 'tomorrow':
            start_date = date.today() + timedelta(days=1)
        else:
            start_date = parser.parse(start_date_str)
        print(start_date)
        
        # Read data from file
        # TODO: if file is empty, give user prompt to enter words
        wotd_list = list()
        with open(input_filepath_wotd) as f:
            for line in f:
                line_final = line.strip()
                wotd_list.append(line_final)
                
        # Combine dates and words
        record_list = list()
        record_date = start_date + timedelta(days=-1)
        for wotd in wotd_list:
            record_date = record_date + timedelta(days=1)
            value_list = list()
            value_list.append(record_date.strftime('%Y-%m-%d'))
            value_list.append(wotd)
            record_list.append(value_list)
            
        # Get SQL query
        result = pack_sql_insert('wotd', attribute_list, record_list)
        
        print(result)

class QueryBuilder:
    def __init__(self):
        pass
    
    def pack_sql_value(self, value):
        if '\'' in value:
            value = value.replace('\'', '\'\'')
        result = '\'' + value + '\''
        return result
    
    def pack_sql_value_header(self, value):
        result = '`' + value + '`'
        return result
    
    def pack_sql_record(self, record_list, header=False):
        count = -1
        result = '('
        for value in record_list:
            count += 1
            if header:
                pack_value = self.pack_sql_value_header(value)
            else:
                pack_value = self.pack_sql_value(value)
            
            if count > 0:
                result += ','
            result += pack_value
        
        result += ')'
        return result
    
    def pack_sql_insert(self, table, attribute_list, record_list):
        result = 'INSERT INTO '
        result += '`' + table + '` '
        
        attributes = self.pack_sql_record(self, attribute_list, True)
        result += attributes
        result += ' VALUES '
        
        record_count = -1
        
        for record in record_list:
            # record_list contains a list within a list
            record_count += 1
            record_packed= ''
            
            if record_count > 0:
                record_packed += ','
            record_packed += '\n'
            
            record_packed += self.pack_sql_record(record)
            result += record_packed
        
        return result

class ZhwotdQuery:

    def select_wotd(self, d=None, w=None):
        """Select an entry from the WOTD database by either date or term
        SELECT * FROM wotd WHERE date = {d}
        SELECT * FROM wotd WHERE word = {w}
        
        Keyword arguments:
        d -- date of Word of the Day
        w -- Word of the Day (can appear on multiple dates)
        """
        
        # Build query
        if d is not None:
            d_str = d.strftime('%Y-%m-%d')
            q = select(wotd).where(wotd.c.date == d_str)
            
            # q = ("SELECT date, word FROM wotd WHERE date = %s")
            # q_tuple = (d.strftime('%Y-%m-%d'),)      # tuple with one value must have a comma...
        elif w is not None:
            q = select(wotd).where(wotd.c.word == w)
            # q = ("SELECT date, word FROM wotd WHERE word = %s")
            # q_tuple = (w,)
        
        # result = query_db(q, q_tuple)
        
        return result
    
    def select_term(self, term):
        # Build query
        q = ("SELECT * FROM dictionary WHERE term = %s")
        q_tuple = (term,)      # tuple with one value must have a comma...
        result = query_db(q, q_tuple)
        
        return result

# DASHBOARD
wotd_db = WOTD_DB()
dict_db = Dictionary_DB()


# TEST
    
def test_function():
    # out-of-date - term input needs to be dict
    t = Term(term="能力")
    result = wotd_db.count_term_in_wotd_db(t)
    
    x = "is not in database"
    if result > 0:
        x = "is in database"
    print("{} {}".format(t.term, x))
    return 

def test_function2():
    # out-of-date - term input needs to be dict
    t = Term(term="能力")
    result = dict_db.is_term_in_dictionary(t)
    
    x = "is not in database"
    if result > 0:
        x = "is in database"
    print("{} {}".format(t.term, x))
    return 

def test_function3():
    config = configparser.ConfigParser()
    config.read("../config.ini")
    print(config['FILES']['input_dir'])
    return

test_function3()