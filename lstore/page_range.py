from lstore.logical_page import LogicalPage
from lstore.config import Config
from lstore.table import Table


class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = [LogicalPage(num_columns) for i in range(16)] # Fixed
        self.tail_pages = [LogicalPage(num_columns)] # Dynamic
        

    def read_record(self, rid): # Read base page
        
        pass
    

    def write_record(self, record): # Create in tail pages, for both update and create
        if not self.tail_pages[-1].has_capacity():
            self.tail_pages[-1].append[LogicalPage(self.num_columns)]
        
        self.tail_pages[-1].create_record(record) # Can raise Error
    
    def delete_record(self): # Flag for deletion in base pages
        
        pass
