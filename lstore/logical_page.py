from config import Config
from physical_page import PhysicalPage
# Represents either a base page or tail page (each set of columns), base/tail page is effectively a row
class LogicalPage:
    
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.physical_pages = [PhysicalPage() for i in range(Config.NUM_META_COLUMNS + num_columns)] # the columns of a record
        self.num_records = 0
    
    def has_capacity(self):
        return self.num_records < Config.MAX_RECORDS_PER_LOGICAL_PAGE
    
    def read_record(self, logical_index):
        record = []
        for physical_page in self.physical_pages:
            column_value = physical_page.read(logical_index) # Can raise Error
            record.append(column_value)

        return record

    def create_record(self, record):
        if not self.has_capacity():
            raise Exception("Cannot write, this logical page is full")

        for i in range(self.num_columns):
            physical_page = self.physical_pages[i]
            physical_page.create(record[i]) # Can raise Error
        self.num_records += 1

    def update_record(self, logical_index, record):
        for physical_page in self.physical_pages:
            physical_page.update(logical_index, record) # Can raise Error
    
    def mark_to_delete_record(self, logical_index):
        self.physical_pages[Config.INDIRECTION_COLUMN].update(logical_index, -1) # Can raise Error