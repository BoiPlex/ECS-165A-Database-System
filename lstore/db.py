from lstore.table import Table
from lstore.config import Config
from lstore.bufferpool import Bufferpool
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from lstore.index import Index

import os
import json
import pickle

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
        if not os.path.exists(self.db_path):
            return tables

        # Iterate through each table directory and load table data
        for table_name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, table_name)
            if os.path.isdir(table_path):
                table = self.read_table(table_name)
                tables[table_name] = table
        return tables

    # Write the given Table object's metadata to disk (next_rid, page_directory, index)
    def write_table_metadata(table):
        
        if not self.path_exists():
            return
        
        table_dir = os.path.join(self.db_path, table.name)
        os.makedirs(table_dir, exist_ok=True)
        
        # Save table metadata
        with open(os.path.join(table_dir, "table.hdr"), "w") as f:
            json.dump({"name": table.name, "key": table.key, "num_columns": table.num_columns, "next_rid": table.next_rid}, f)
        
        # Save page directory
        with open(os.path.join(table_dir, "page_directory.json"), "w") as f:
            json.dump(table.page_directory, f)
        
        # Save index using pickle serialization
        with open(os.path.join(table_dir, "index.pickle"), "wb") as f:
            pickle.dump(table.index, f)
        
        # Write next_rid to table.hdr
        # Write page directory to page_directory.json
        # Use pickle to serialize index and write index.pickle
        
        pass

    # Read table from disk and return Table object
    def read_table(table_name):
        table_dir = os.path.join(self.db_path, table_name)
        if not os.path.exists(table_dir):
            return None
        
        # Load metadata
        with open(os.path.join(table_dir, "table.hdr"), "r") as f:
            metadata = json.load(f)
        
        # Load page directory
        with open(os.path.join(table_dir, "page_directory.json"), "r") as f:
            page_directory = json.load(f)
        
        # Load index using pickle
        with open(os.path.join(table_dir, "index.pickle"), "rb") as f:
            index = pickle.load(f)
        
        table = Table(metadata["name"], metadata["num_columns"], metadata["key"], None)
        table.next_rid = metadata["next_rid"]
        table.page_directory = page_directory
        table.index = index
        return table

    
    # Read logical page from disk and return LogicalPage object
    def read_logical_page(table_name, page_range_index, record_type, logical_page_index):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index), record_type, str(logical_page_index))
        
        with open(path, "rb") as f:
            logical_page = pickle.load(f)
        return logical_page

    # Write the given LogicalPage object to disk
    def write_logical_page(table_name, page_range_index, record_type, logical_page_index, logical_page):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index), record_type, str(logical_page_index))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "wb") as f:
            pickle.dump(logical_page, f)

    def insert_page_range(table_name, page_range_index):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index))
        os.makedirs(path, exist_ok=True)
        
        # Create a metadata file for the new page range
        with open(os.path.join(path, "page_range.hdr"), "w") as f:
            json.dump({"num_base_records": 0, "num_tail_pages": 0, "num_updates": 0}, f)
    
    def insert_logical_page(table_name, page_range_index, record_type):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index))
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "page_range.hdr"), "w") as f:
            json.dump({"num_base_records": 0, "num_tail_pages": 0, "num_updates": 0}, f)


    # # For creating any directory such as the db dir or page_range dir
    # def create_directory(path):
    #     os.makedirs(path, exist_ok=True)