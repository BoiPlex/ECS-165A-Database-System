from lstore.config import Config
from lstore.table import Table
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from lstore.physical_page import PhysicalPage

import os
import json
import pickle

class Disk():
    def __init__(self, db_path):
        self.db_path = db_path
    
    # Read db from disk and return tables dict (table name => Table())
    def read_db(self, bufferpool):
        tables = {}
        if not os.path.exists(self.db_path):
            return tables

        # Iterate through each table directory and load table data
        for table_name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, table_name)
            if os.path.isdir(table_path):
                table = self.read_table(table_name, bufferpool)
                tables[table_name] = table
        return tables
    
    # Write the given Table object's metadata to disk (next_rid, page_directory, index) and also its page range's 
    def write_table_and_page_ranges_metadata(self, table):
        if not self.path_exists():
            return
        
        # Get table path
        table_path = os.path.join(self.db_path, table.name)
        os.makedirs(table_path, exist_ok=True)

        table_header = {
            "name": table.name,
            "key": table.key,
            "num_columns":table.num_columns,
            "next_rid":table.next_rid
        }
        
        # Save table header and page directory as pkl files
        self.write_python_dict_as_file(table_path, table_header, "header.pkl")
        self.write_python_dict_as_file(table_path, table.page_directory, "page_directory.pkl")
        # write index pkl
        with open(os.path.join(table_path, "index.pkl"), "wb") as f:
            pickle.dump(table.index, f)

        # loop through page_ranges and headers
        page_ranges_path = os.path.join(table_path, "page_ranges")
        os.makedirs(page_ranges_path, exist_ok=True)
        for page_range_index in range(len(table.page_ranges)):
            page_range_path = os.path.join(page_ranges_path, str(page_range_index))
            page_range: PageRange = table.page_ranges[page_range_index]
            
            page_range_header = {
                "table_name": table.name,
                "page_range_index": page_range_index,
                "num_columns": page_range.num_columns,
                "num_base_records": page_range.num_base_records,
                "num_updates":page_range.num_updates,
            }
            os.makedirs(page_range_path, exist_ok=True)
            self.write_python_dict_as_file(page_range_path, page_range_header, "header.pkl")
                

    #call? read_file_as_python_dict*** use for pkl NO index

    #def write_page_range_metadata(self,table_name,page_range_index) ?
    #writes metadata for single page range in header pkl

    #def get_page_range_from_bufferpool 

    # Read table from disk and return Table object
    def read_table(self, table_name, bufferpool):
        table_path = os.path.join(self.db_path, table_name)
        if not os.path.exists(table_path):
            return None
        
        # Gets table_header and page_directory data
        table_header = self.read_file_as_python_dict(table_path, "header.pkl")
        page_directory = self.read_file_as_python_dict(table_path, "page_directory.pkl")

        # Ensures that path to "index.pkl" exists - returns None if it does not, builds index_object if it does
        index_path = os.path.join(table_path, "index.pkl")
        if not os.path.exists(index_path):
            return None
        with open(index_path, "rb") as f:
            index_object = pickle.load(f)

        # Builds table from data in table directory
        table = Table(table_header["name"], table_header["num_columns"], table_header["key"], bufferpool)
        table.next_rid = table_header["next_rid"]
        table.page_directory = page_directory
        table.index = index_object

        # Fills table with page ranges stored in its directory
        page_ranges_path = os.path.join(table_path, "page_ranges") 
        for page_range_index in sorted(os.listdir(page_ranges_path)):
            page_range_header = self.read_file_as_python_dict(page_ranges_path, page_range_index)
            page_range = PageRange(page_range_header["table_name"], page_range_header["page_range_index"], page_range_header["num_columns"], bufferpool)
            page_range.num_base_records = page_range_header["num_base_records"]
            page_range.num_updates = page_range_header["num_updates"]
            table.page_ranges.append(page_range)
        
        # Returns fully built table
        return table

    # Read logical page from disk and return LogicalPage object
    def read_logical_page(self, table_name, page_range_index, record_type, logical_page_index):
        logical_pages_dir = "base_pages" if record_type == Config.BASE_RECORD else "tail_pages"
        logical_page_path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index), logical_pages_dir, str(logical_page_index))
        
        # Should return None if logical page DNE
        if not os.path.exists(logical_page_path):
            return None
        
        # Gets header for logical page
        logical_page_header = self.read_file_as_python_dict(logical_page_path, "header.pkl")
        
        # Builds logical page and subsequent physical pages 
        logical_page = LogicalPage(logical_page_header["num_columns"])
        logical_page.num_records = logical_page_header["num_records"]
        
        physical_pages_path = os.path.join(logical_page_path, "physical_pages")
        if not os.path.exists(physical_pages_path):
            return None
        
        physical_pages = []
        for column_index in range(logical_page.num_columns):
            physical_page_path = os.path.join(physical_pages_path, str(column_index))
            if not os.path.isdir(physical_page_path):
                continue
            physical_page_header_path = os.path.join(physical_page_path, "header.pkl")
            physical_page_data_path = os.path.join(physical_page_path, "physical_page.data")
            
            physical_page_header = self.read_file_as_python_dict(physical_page_header_path, "header.pkl")
            if not physical_page_header:
                return None
            
            if not os.path.exists(physical_page_data_path):
                return None
            with open(physical_page_data_path, "rb") as f:
                physical_page_data = pickle.load(f)
            
            physical_page = PhysicalPage()
            physical_page.num_records = physical_page_header["num_records"]
            physical_page.data = bytearray(physical_page_data)

            physical_pages.append(physical_page)
            
        logical_page.physical_pages = physical_pages

        return logical_page

    # Write the given LogicalPage object to disk
    def write_logical_page(self, table_name, page_range_index, record_type, logical_page_index, logical_page: LogicalPage):
        logical_pages_dir = "base_pages" if record_type == Config.BASE_RECORD else "tail_pages"
        logical_page_path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index), logical_pages_dir, str(logical_page_index))
        os.makedirs(logical_page_path, exist_ok=True)

        logical_page_header = {
            "num_columns": logical_page.num_columns,
            "num_records": logical_page.num_records,
        }
        self.write_python_dict_as_file(logical_page_path, logical_page_header, "header.pkl")

        physical_pages_path = os.path.join(logical_page_path, "physical_pages")
        for column_index in range(logical_page.num_columns):
            physical_page_path = os.path.join(physical_pages_path, str(column_index))
            physical_page = logical_page.physical_pages[column_index]

            physical_page_header = {
                "num_records": physical_page.num_records,
            }
            
            os.makedirs(physical_page_path, exist_ok=True)
            self.write_python_dict_as_file(physical_page_path, physical_page_header, "header.pkl")

            physical_page_data_path = os.path.join(physical_page_path, "physical_page.data")
            with open(physical_page_data_path, "wb") as f:
                f.write(physical_page.data)

    def insert_page_range(self, table_name, page_range_index):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index))
        os.makedirs(path, exist_ok=True)
        
        # Create a metadata file for the new page range
        with open(os.path.join(path, "page_range.hdr"), "w") as f:
            json.dump({"num_base_records": 0, "num_tail_pages": 0, "num_updates": 0}, f)
    
    def insert_logical_page(self, table_name, page_range_index, record_type):
        path = os.path.join(self.db_path, table_name, "page_ranges", str(page_range_index))
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "page_range.hdr"), "w") as f:
            json.dump({"num_base_records": 0, "num_tail_pages": 0, "num_updates": 0}, f)

    # Read pkl file, deserialize into json, and convert to python dict. Return python dict
    def read_file_as_python_dict(self, path: str, filename: str):        
        file_path = os.path.join(path, filename)
        
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, "rb") as f:
            json_data = pickle.load(f)
        
        data = json.loads(json_data)
        return data
    
    def write_python_dict_as_file(self, path: str, data: dict, filename: str):
        if not os.path.exists(path):
            return

        file_path = os.path.join(path, filename)
        json_data = json.dumps(data)

        with open(file_path, "wb") as f:
            pickle.dump(json_data, f)

    # Return bool whether path exists on disk
    def path_exists(self):
        return self.db_path != None and os.path.exists(self.db_path)