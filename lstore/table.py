from lstore.index import Index
from lstore.physical_page import PhysicalPage
from lstore.config import Config
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns



class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # Maps RID -> (Page Range Index, Base Page Index, Offset)
        self.index = Index(self)
        
        self.next_rid = 0
        self.page_ranges = [PageRange(num_columns)]
        pass

    
    """
    RID -> [which page range -> which BP of 16 -> which index in BP]
    {RID: page_range, base_page, index}
    
    
    """

    def read_record(self, rid):
        
        if rid not in self.page_directory:
            raise ValueError("RID cannot be found in page directory.")

        page_range_index, base_page_index, physical_page_index = self.page_directory[rid] # Find the page range that contains the RID in the page directory
        base_page = self.page_ranges[page_range_index].base_pages[base_page_index]
        record = [physical_page[physical_page_index] for physical_page in base_page.physical_pages]

        # record_metadata = record[:4]
        # record_data = record[4:]
        
        return record

    def create_record(self, record_data):
        rid = self.next_rid
        self.next_rid += 1


        # Stores index data for respective record ID
        page_range_index = self.page_ranges[...]
        base_page_index, physical_page_index = ()
        self.page_directory[rid] = (page_range_index, base_page_index, physical_page_index)
        
        page_ranges

        LogicalPage()
        
        pass

    def update_record(self, rid, record_data):
        pass
    
    # For now just mark as deleted, will be fully implemented with __merge(self)
    def delete_record(self, rid):
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
