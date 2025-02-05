from lstore.logical_page import LogicalPage
from lstore.config import Config
from lstore.table import Table


class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = [LogicalPage(num_columns) for i in range(16)] # Fixed
        self.tail_pages = [LogicalPage(num_columns)] # Dynamic
        self.num_records = 0

    def has_capacity(self):
        return self.num_records < Config.MAX_RECORDS_PER_PAGE_RANGE
        

    def read_record(self, base_page_index): # Read base page
        record = []
        for base_page in self.base_pages:
            logical_page = base_page.read(base_page_index) # Can raise Error
            

        return record
    
    def create_record(self, record): # Create NEW record in base page
        if not self.base_pages[-1].has_capacity():
            self.base_pages[-1].append[LogicalPage(self.num_columns)]
        
        self.base_pages[-1].create_record(record) # Can raise Error
        self.num_records += 1

    def update_record(self, ) # Create record in tail page to overwrite records on merge
        if not self.tail_pages[-1].has_capacity():
            self.tail_pages[-1].append[LogicalPage(self.num_columns)]
        
        self.tail_pages[-1].create_record(record) # Can raise Error
        
    
    def mark_to_delete_record(self): # Flag for deletion in base pages, full deletion only happens on merge
        
        pass
