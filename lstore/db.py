from lstore.table import Table
from lstore.config import Config
from lstore.bufferpool import Bufferpool

class Database():

    def __init__(self):
        self.tables = {}
        self.disk = None

    # Checks the given path. The path could either already contain a db or one must be created
    def open(self, path):
        pass
    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return None
        
        self.bufferpool = Bufferpool()

        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name not in self.tables:
            return
    
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name, None)


class Disk():
    def __init__(self, db_path):
        self.db_path = db_path
    
    # Read the whole table from disk and into memory
    def read_db():
        pass

    # Write the whole table from memory and into disk
    def write_db():
        pass
    
    # Read logical page from disk and return the LogicalPage object
    def read_logical_page(page_range_index, record_type, logical_page_index):
        pass

    # Take a LogicalPage object and write it to disk
    def write_logical_page(page_range_index, record_type, logical_page_index, logical_page):
        pass

    # Insert new tail page (no need for a func for inserting base pages bc they're fixed)
    def insert_tail_page():
        pass

    def insert_page_range():
        pass
