from lstore.config import Config
from lstore.physical_page import PhysicalPage

#import json

# Represents either a base page or tail page (each set of columns), base/tail page is effectively a row
class LogicalPage:
    
    # num_columns should include the 4 meta-columns
    def __init__(self, num_columns):
        self.num_columns = num_columns

        self.physical_pages = [PhysicalPage() for i in range(num_columns)] # the columns of a record
        self.num_records = 0
    
    def has_capacity(self):
        return self.num_records < Config.MAX_RECORDS_PER_LOGICAL_PAGE
    
    def read_record(self, offset_index):
        record = []
        for physical_page in self.physical_pages:
            column_value = physical_page.read(offset_index) # Can raise Error
            record.append(column_value)

        return record

    def create_record(self, columns):
        if not self.has_capacity():
            raise Exception("Cannot write, this logical page is full")

        for i in range(self.num_columns):
            physical_page = self.physical_pages[i]
            offset_index = physical_page.create(columns[i]) # Can raise Error
        self.num_records += 1

        return offset_index

    # Updates the value of one column of a record (row)
    def update_record_value(self, offset_index, column_index, column_value):
        self.physical_pages[column_index].update_value(offset_index, column_value)
    
    # RID of 0 is reserved for indicating deletion
    def mark_to_delete_record(self, offset_index):
        self.physical_pages[Config.INDIRECTION_COLUMN].update_value(offset_index, 0) # Can raise Error

    

    '''
    Column Values
    -----------------
    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3
    '''