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
import sqlalchemy

# GENERAL DATABASE FUNCTIONS
class DatabaseManager:
    def __init__(self):
        # Load config file to get database connection details
        self.config = configparser.ConfigParser()
        self.config.read("../config.ini")
        db_file = self.config["FILES"]["db_file"]
        db_file_dir = self.config["FILES"]["input_dir"]
        self.db_file_fullpath = join(db_file_dir, db_file)
        self.db_json = json.load(self.db_file_fullpath)
    
    def configure_engine(self):
        """With details from config file, configure a SQLAlchemy engine
        
        https://docs.sqlalchemy.org/en/14/core/engines.html
        """
        u = self.db_json['user']
        p = self.db_json['password']
        h = self.db_json['host']
        d = self.db_json['database']
        db_url = 'mysql://' + u + ':' + p + '@' + h + '/' + d
        engine = sqlalchemy.create_engine(db_url)
        return engine


# WOTD

class WOTD:
    """Word of the Day entry
    
    Attributes:
        d: date of entry
        word: the word of the day itself
    """
    
    def __init__(self, d=None, word=None):
        self.d = d
        self.word = word


class WOTD_DB:
    """Class that represents online Word of the Day database
    """
    
    def __init__(self):
        self.table_name = "wotd"
        self.dm = DatabaseManager()
        self.date_max = self.get_date_max()
        self.date_min = self.get_date_min()
        
    def q_count_all_entries(self, term):
        # Query 'wotd' table for all; count
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_wotd = sqlalchemy.Table('wotd', metadata, autoload_with=engine)
        q = sqlalchemy.select(sqlalchemy.func.count()).select_from(table_wotd)
        
        with engine.begin() as conn:
            result = conn.execute(q).scalar()
        
        return result
    
    def q_date_has_entry(self, d):
        """Check if there is an existing entry for the given date
        """
        
        result = False
        d_str = d.strftime('%Y-%m-%d')
        
        # Query 'wotd' table for this date; if >= 1 row, set result to true
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_wotd = sqlalchemy.Table('wotd', metadata, autoload_with=engine)
        col_date = sqlalchemy.column('date')
        q = table_wotd.select().where(col_date == d_str)
        
        with engine.begin() as conn:
            results = conn.execute(q)
        
        if len(results) > 0:
            result = True
        
        return result
    
    def q_count_word_instances(self, word):
        """Count number of times a given word has been used as WOTD
        """
        
        # Query 'wotd' table for this word; count
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_wotd = sqlalchemy.Table('wotd', metadata, autoload_with=engine)
        col_word = sqlalchemy.column('word')
        q = sqlalchemy.select(sqlalchemy.func.count()).select_from(table_wotd).where(col_word == word)
        
        with engine.begin() as conn:
            result = conn.execute(q).scalar()
        
        return result
    
    def q_find_missing_date(self):
        """Find any dates that have been skipped between the current min and max date entries
        """
        # TODO: find_missing_date()
        return None
    
    def q_get_date_max(self):
        """Find the max date in the wotd table
        """
        
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_wotd = sqlalchemy.Table('wotd', metadata, autoload_with=engine)
        col_date = sqlalchemy.column('date')
        
        q = sqlalchemy.select(sqlalchemy.func.max(col_date)).select_from(table_wotd)
        with engine.begin() as conn:
            result = conn.execute(q).scalar()
    
        return result
    
    def q_get_date_min(self):
        """Find the min date in the wotd table
        """
        
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_wotd = sqlalchemy.Table('wotd', metadata, autoload_with=engine)
        col_date = sqlalchemy.column('date')
        
        q = sqlalchemy.select(sqlalchemy.func.min(col_date)).select_from(table_wotd)
        with engine.begin() as conn:
            result = conn.execute(q).scalar()
    
        return result
    

# DICTIONARY

class Dictionary_DB:
    """Class that represents online dictionary database
    """
    
    def __init__(self):
        self.table_name = "dictionary"
    
    def q_get_term(self, term):
        """Return dictionary entry for term
        """
    
    def q_is_term_in_dictionary(self, term):
        """Check if term is in dictionary
        """
        
        # Query 'wotd' table for this date; if >= 1 row, set result to true
        result = False
        engine = self.dm.configure_engine()
        metadata = sqlalchemy.MetaData()
        table_dictionary = sqlalchemy.Table('dictionary', metadata, autoload_with=engine)
        col_term = sqlalchemy.column('term')
        q = table_dictionary.select().where(col_term == term)
        
        with engine.begin() as conn:
            results = conn.execute(q)
        
        if len(results) > 0:
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
    """Processes files for bulk importing of dictionary and WOTD data
    """
    def __init__(self):
        self.p = PinyinParser()
        self.get_input_file_settings()
    
    def get_input_file_settings(self):
        """Get info about where to load files
        """
        
        # TODO: too much hardcoding; offload somewhere (config file?)
        # TODO: maybe absorb back into __init__
        input_dir = expanduser('~')
        input_dir = join(input_dir,'Dropbox','zhwotd')
        
        input_file_dictionary = 'zhwotd_input_dictionary.csv'
        self.input_filepath_dictionary = join(input_dir, input_file_dictionary)
        
        input_file_wotd = 'zhwotd_input_wotd.txt'
        self.input_filepath_wotd = join(input_dir, input_file_wotd)
        
        
    def parse_dictionary_input_csv(self, input_filepath):
        """Parse input from csv file to import to dictionary table
        """
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
                            # Parse pinyin entry (e.g., convert from tone numbers to diacritics)
                            col = self.p.parse_pinyin_entry(col)
                        elif col_count == col_hsk and col_count == '':
                            # if this entry is blank, set it to zero
                            # TODO: if entry is anything but 1 to 5, set it to zero
                            col = '0'
    
                        row_result.append(col)
                        
                
                # Pack row_result into record_list
                if row_count > 0:
                    record_list.append(row_result)
        
        # After extracting info from file, create the SQL query
        # TODO: replace with SQLAlchemy equivalent; maybe goes to ZhwotdQuery()
        result = pack_sql_insert('dictionary', header, record_list)
                    
        return result
    
    
    def parse_wotd_input(self, input_filepath):
        """Parse input from file (csv or txt) to import to wotd table
        """
        
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


# DASHBOARD
wotd_db = WOTD_DB()
dict_db = Dictionary_DB()


# TEST
    
