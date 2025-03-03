from lstore.config import Config
from lstore.bufferpool import Bufferpool
from lstore.table import Table
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from lstore.index import Index
from lstore.disk import Disk

import os

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
            self.tables = self.disk.read_db(self.bufferpool)
        # If not, create new disk dir
        else:
            os.makedirs(path, exist_ok=True)
            self.tables = {}

    # Close the db, write memory to disk for durable storage
    def close(self):
        if not self.disk.path_exists():
           return

        for table in self.tables.values():
            self.disk.write_table_and_page_ranges_metadata(table)
        
        self.bufferpool.write_back_all_dirty_frames()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):        
        # if not self.disk.path_exists():
        #     return None
        
        if name in self.tables:
            return self.tables[name]

        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    # Once deleted, the table won't be saved to disk since it's not in self.tables anymore
    """
    def drop_table(self, name):
        # if not self.disk.path_exists():
        #     return
        
        if name not in self.tables:
            return
        
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name, None)
    
