from logical_page import LogicalPage
from config import Config

class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns # Includes 4 meta columns
        self.base_pages = [LogicalPage(num_columns) for i in range(16)] # Fixed
        self.tail_pages = [LogicalPage(num_columns)] # Dynamic
        self.num_base_records = 0 # (We don't keep track of tail records because they're theoretically infinite)

    def has_capacity(self):
        return self.num_base_records < Config.MAX_RECORDS_PER_PAGE_RANGE

    # Read base/tail record
    def read_record(self, record_type, logical_page_index, offset_index):
        logical_page = self.get_logical_pages(record_type)[logical_page_index]
        return logical_page.read_record(offset_index)
    
    # Create NEW record (base or tail)
    def create_record(self, record_type, record_columns):
        logical_pages = self.get_logical_pages(record_type)

        # BASE_RECORD
        if record_type == Config.BASE_RECORD:
            logical_page_index = self.find_free_base_page()
            # No space in any of the base pages
            if logical_page_index < 0:
                raise Exception("No space in any of the base pages")
        # TAIL RECORD
        else:
            if not logical_pages[-1].has_capacity():
                logical_pages.append(LogicalPage(self.num_columns))
            logical_page_index = len(self.tail_pages) - 1 
        
        # Create base/tail record
        offset_index = logical_pages[logical_page_index].create_record(record_columns) # Can raise Error
        if record_type == Config.BASE_RECORD:
            self.num_base_records += 1

        # Return the index of the base/tail page the record was written to and offset index in the base/tail page
        return logical_page_index, offset_index

    # Update single column value of a tail record to overwrite (TAIL RECORD MUST ALREADY EXIST)
    def update_tail_record_value(self, tail_page_index, offset_index, column_index, column_value):
        self.tail_pages[tail_page_index].update_record_value(offset_index, column_index, column_value)

    def mark_to_delete_record(self, record_type, logical_page_index, offset_index): # Flag for deletion of record (base or tail), full deletion only happens on merge        
        logical_page = self.get_logical_pages(record_type)[logical_page_index]
        logical_page.mark_to_delete_record(offset_index)
    
    def get_logical_pages(self, record_type):
        # BASE_RECORD
        if record_type == Config.BASE_RECORD:
            return self.base_pages
        # TAIL_RECORD
        else:
            return self.tail_pages
    
    # Iterate through the base pages to find one with space
    # Returns base_page_index if successful, -1 otherwise
    def find_free_base_page(self):
        for base_page_index, base_page in enumerate(self.base_pages):
            if base_page.has_capacity():
                return base_page_index
        return -1
