from lstore.table import Table
from lstore.config import Config

class Database():

    def __init__(self):
        self.tables = {}
        pass

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
