from lstore.index import Index
from lstore.physical_page import PhysicalPage
from lstore.config import Config
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from time import time


from BTrees.OOBTree import BTree


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
        
        # Maps RID -> (Page Range Index, Logical Page Index, Page Offset)
        self.page_directory = BTree()

        self.index = Index(self)
        
        self.next_rid = 0
        self.page_ranges = [PageRange(self.num_columns)]

    def read_value(self, rid):
        
        if rid not in self.page_directory:
            raise ValueError("RID cannot be found in page directory.")

        page_range_index, logical_page_index, physical_page_index = self.page_directory[rid]
        base_page = self.page_ranges[page_range_index].base_pages[logical_page_index]
        record = [physical_page[physical_page_index] for physical_page in base_page.physical_pages]

        # record_metadata = record[:4]
        # record_data = record[4:]
        
        return record

    def create_record(self, record_data):
        rid = self.next_rid
        self.next_rid += 1

        page_range_index = -1

        for i, page_range in enumerate(self.page_ranges):
            if page_range.has_capacity():
                page_range_index = i
                break
        
        # If no more pages, create new page range
        if page_range_index == -1:
            new_page_range = PageRange(self.num_columns)
            self.page_ranges.append(new_page_range)
            page_range_index = len(self.page_ranges) - 1

        base_page_index, physical_page_index = new_page_range.write_record(record_data)
        self.page_directory[rid] = (page_range_index, base_page_index, physical_page_index)

    def update_record(self, rid, record_data):
        rid = self.next_rid
        self.next_rid += 1

        for i, page_range in enumerate(self.page_ranges):
            if page_range.has_capacity():
                page_range_index = i
                
        pass
    
    # For now just mark as deleted, will be fully implemented with __merge(self)
    def delete_record(self, rid):
        page_range_index, base_page_index, physical_page_index = self.page_directory[rid]
        

    def __merge(self):
        print("merge is happening")
        pass
 
