from lstore.table import Table
from lstore.config import Config
from lstore.bufferpool import Bufferpool

import os
import json

class Database():

    def __init__(self):
        self.path = None
        self.disk = None
        self.bufferpool = None
        self.tables = {}

    # Checks the given path. The path could either already contain a db or one must be created
    def open(self, path):
        self.path = path
        self.disk = Disk(path)
        self.bufferpool = Bufferpool(self.disk)

        # If disk dir exists, read it
        if os.path.exists(path):
            self.tables = self.disk.read_db()

        # If not, create new disk dir
        else:
            os.makedirs(path, exist_ok=True)
            self.tables = {}

    # Close the db, write memory to disk for durable storage
    def close(self):
        if not self.path_exists():
            return

        for table in self.tables.values():
            self.disk.write_table_metadata(table)
        
        self.bufferpool.write_back_all_dirty_pages()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):        
        if not self.path_exists() or name in self.tables:
            return None

        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    # Once deleted, the table won't be saved to disk since it's not in self.tables anymore
    """
    def drop_table(self, name):
        if not self.path_exists() or name not in self.tables:
            return
        
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name, None)
    
    # Return bool whether path exists on disk
    def path_exists(self):
        return self.path != None and os.path.exists(self.path)


class Disk():
    def __init__(self, db_path):
        self.db_path = db_path
    
    # Read db from disk and return tables dict (table name => Table())
    def read_db():
        tables = {}
        
        return tables

    # Write the given Table object's metadata to disk (next_rid, page_directory, index)
    def write_table_metadata(table):
        
        # Write next_rid to table.hdr
        # Write page directory to page_directory.json
        # Use pickle to serialize index and write index.pickle
        
        pass

    # Read table from disk and return Table object
    def read_table(table_name):
        # Read everything to create table object
        pass
    
    # Read logical page from disk and return LogicalPage object
    def read_logical_page(table_name, page_range_index, record_type, logical_page_index):
        pass

    # Write the given LogicalPage object to disk
    def write_logical_page(table_name, page_range_index, record_type, logical_page_index, logical_page):
        pass

    def insert_page_range(table_name, page_range_index):
        #with open(self., 'w') as !!
        pass

    def insert_logical_page(table_name, page_range_index, record_type):
        pass

    # # For creating any directory such as the db dir or page_range dir
    # def create_directory(path):
    #     os.makedirs(path, exist_ok=True)