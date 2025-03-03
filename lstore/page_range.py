from lstore.config import Config
from lstore.bufferpool import Bufferpool
from lstore.logical_page import LogicalPage


class PageRange:
    def __init__(self, table_name, page_range_index, num_columns, bufferpool):
        self.table_name = table_name
        self.page_range_index = page_range_index
        self.num_columns = num_columns # Includes meta columns
        
        self.bufferpool = bufferpool # Allows access to logical pages in the bufferpool

        # self.num_base_pages = 0 # Tracks number of base pages in page range
        # self.num_tail_pages = 0 # Tracks number of tail pages in page range

        self.num_base_records = 0 # Tracks number of base records in page range 
        self.num_tail_records = 0
        self.num_updates = 0 # Merge the page range once it reaches Config.NUM_UPDATES_FOR_MERGE

    def has_capacity(self):
        return self.num_base_records < Config.MAX_RECORDS_PER_PAGE_RANGE

    # Read base/tail record
    def read_record(self, record_type, logical_page_index, offset_index):
        logical_page_frame = self.bufferpool.request_logical_page_frame(
            self.num_columns,
            self.table_name,
            self.page_range_index,
            record_type,
            logical_page_index
        )
        self.bufferpool.pin_frame(logical_page_frame)
        record = logical_page_frame.logical_page.read_record(offset_index)
        self.bufferpool.unpin_frame(logical_page_frame)
        return record
    
    # Create NEW record (base or tail)
    def create_record(self, record_type, record_columns):
        logical_page_index = self.find_free_logical_page(record_type) # Finds free logical page / creates new one
        
        frame = self.bufferpool.request_logical_page_frame(self.num_columns, self.table_name, self.page_range_index, record_type, logical_page_index)
        self.bufferpool.pin_frame(frame)
        frame.dirty = True
        # Create base/tail record
        offset_index = frame.logical_page.create_record(record_columns) # Can raise Error
        self.bufferpool.unpin_frame(frame)
        
        # Return the index of the base/tail page the record was written to and offset index in the base/tail page
        return logical_page_index, offset_index
    
    # Read single column value of a record (RECORD MUST ALREADY EXIST)
    def read_record_column(self, record_type, logical_page_index, offset_index, column_index):
        logical_page_frame = self.bufferpool.request_logical_page_frame(
            self.num_columns,
            self.table_name,
            self.page_range_index,
            record_type,
            logical_page_index
        )
        self.bufferpool.pin_frame(logical_page_frame)
        
        column_value = logical_page_frame.logical_page.physical_pages[column_index].read(offset_index)

        self.bufferpool.unpin_frame(logical_page_frame)

        return column_value

    # Update single column value of a record to overwrite (RECORD MUST ALREADY EXIST)
    def update_record_column(self, record_type, logical_page_index, offset_index, column_index, column_value):
        logical_page_frame = self.bufferpool.request_logical_page_frame(
            self.num_columns,
            self.table_name,
            self.page_range_index,
            record_type,
            logical_page_index
        )
        self.bufferpool.pin_frame(logical_page_frame)
        logical_page_frame.dirty = True

        logical_page_frame.logical_page.update_record_value(offset_index, column_index, column_value)

        self.bufferpool.unpin_frame(logical_page_frame)

    def mark_to_delete_record(self, record_type, logical_page_index, offset_index): # Flag for deletion of record (base or tail), full deletion only happens on merge        
        logical_page_frame = self.bufferpool.request_logical_page_frame(
            self.num_columns,
            self.table_name,
            self.page_range_index,
            record_type,
            logical_page_index
        )
        self.bufferpool.pin_frame(logical_page_frame)
        logical_page_frame.dirty = True
        logical_page_frame.logical_page.mark_to_delete_record(offset_index)
        self.bufferpool.unpin_frame(logical_page_frame)
     
    # Iterate through the base pages to find one with space
    # Returns base_page_index if successful, -1 otherwise
    def find_free_logical_page(self, record_type):
        num_logical_records = self.num_base_records if record_type == Config.BASE_RECORD else self.num_tail_records
        free_logical_page_index = num_logical_records // Config.MAX_RECORDS_PER_LOGICAL_PAGE
        
        return free_logical_page_index