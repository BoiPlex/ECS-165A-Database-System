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

        self.key = key # Key column index (doesn't include meta-columns)

        self.num_columns = num_columns
        
        # Maps RID -> (page_range_index, logical_page-index, offset_index)
        self.page_directory = {}

        self.index = Index(self)
        
        self.next_rid = 1 # RID of 0 is reserved to indicate deletion

        self.page_ranges = [PageRange(self.num_columns + Config.NUM_META_COLUMNS)]

    # Reads base record (includes meta-columns)
    # rid starts at 1 as defined above
    def read_record(self, rid):
        if rid not in self.page_directory:
            return False # Invalid base record rid
    
        page_range_index, logical_page_index, offset_index = self.page_directory[rid]
        record_columns = self.page_ranges[page_range_index].read_record(Config.BASE_RECORD, logical_page_index, offset_index)

        record_metadata = record_columns[:4]
        record_data = record_columns[4:]

        record = Record(rid, record_columns[self.key], record_data)
        
        return record

    # Creates NEW base record
    def create_record(self, record_nonmeta_columns):
        if len(record_nonmeta_columns) != self.num_columns:
            return False # Record could not be created because it does not match the number of non-meta columns in the table

        page_range_index = self.find_free_page_range()
        if page_range_index < 0:
            # Allocate new page range
            self.page_ranges.append(PageRange(self.num_columns + Config.NUM_META_COLUMNS))
            page_range_index = len(self.page_ranges) - 1

        # Get unique RID
        base_rid = self.allocate_rid()
        
        record_columns = self.__initialize_record_columns(base_rid, record_nonmeta_columns)

        # Create record and add to page directory
        logical_page_index, offset_index = self.page_ranges[page_range_index].create_record(Config.BASE_RECORD, record_columns)
        self.page_directory[base_rid] = (page_range_index, logical_page_index, offset_index)

        # Add to index
        self.index.create_index(base_rid, record_nonmeta_columns)

        return True # Record was created

    # Update a record's column value (either creates new tail record or updates existing tail record)
    # nonmeta_column_index shouldn't account for the meta columns (0 for first non-meta column)
    def update_record(self, base_rid, nonmeta_column_index, column_value):
        if base_rid not in self.page_directory:
            return False # Invalid base record rid
    
        column_index = nonmeta_column_index + Config.NUM_META_COLUMNS
        bit_column_index = (self.num_columns + Config.NUM_META_COLUMNS) - column_index - 1

        # # Check if the base record has a tail record that already exists
        # if tail_rid != base_rid and tail_rid in self.page_directory:
        #     # Update base record's schema encoding column
        #     schema_encoding_column_value = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(offset_index)
        #     schema_encoding_column_value |= (1 << bit_column_index)
        #     self.page_ranges[page_range_index].update_tail_record_value(tail_page_index, offset_index, column_index, schema_encoding_column_value)

        #     # Update existing tail record
        #     page_range_index, tail_page_index, offset_index = self.page_directory[tail_rid]
        #     self.page_ranges[page_range_index].update_tail_record_value(tail_page_index, offset_index, column_index, column_value)

        # Get unique RID
        tail_rid = self.allocate_rid()

        # Base record's indexes
        page_range_index, base_page_index, offset_index = self.page_directory[base_rid]

        # Update base record's schema encoding column
        schema_encoding_column_value = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(offset_index)
        schema_encoding_column_value |= (1 << bit_column_index)
        self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].update_value(offset_index, schema_encoding_column_value)
        
        # Lineage to get latest tail record (or could be base record if first update)
        current_rid = base_rid
        follow_rid = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        
        is_base_record = True
        while current_rid != follow_rid:
            if follow_rid not in self.page_directory:
                break
            is_base_record = False

            page_range_index, tail_page_index, offset_index = self.page_directory[follow_rid]
            current_rid = follow_rid
            follow_rid = self.page_ranges[page_range_index].tail_pages[tail_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        
        # Update indirection of latest record
        if is_base_record:
            self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].update_value(offset_index, tail_rid)
        else:
            self.page_ranges[page_range_index].tail_pages[tail_page_index].physical_pages[Config.INDIRECTION_COLUMN].update_value(offset_index, tail_rid)
            
        # Get latest record and update its column
        record_copy = self.read_record(current_rid)
        new_nonmeta_columns = record_copy.columns
        new_nonmeta_columns[nonmeta_column_index] = column_value

        # Create new tail record with given column value
        record_columns = self.__initialize_record_columns(tail_rid, new_nonmeta_columns)
        self.page_ranges[page_range_index].create_record(Config.TAIL_RECORD, record_columns)    
        
        return True
    
    # Deletes base record (along with associated tail record)
    # For now just mark as deleted, will be fully implemented with __merge(self)
    def delete_record(self, base_rid):
        if base_rid not in self.page_directory:
            return False # Invalid rid
    
        # Delete base record
        page_range_index, base_page_index, offset_index = self.page_directory[base_rid]
        self.page_ranges[page_range_index].mark_to_delete_record(Config.BASE_RECORD, base_page_index, offset_index)
        del self.page_directory[base_rid]

        # Check if the base record has a tail record that already exists
        tail_rid = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        if tail_rid in self.page_directory:
            # Delete associated tail record
            page_range_index, tail_page_index, offset_index = self.page_directory[tail_rid]
            self.page_ranges[page_range_index].mark_to_delete_record(Config.TAIL_RECORD, tail_page_index, offset_index)
            del self.page_directory[tail_rid]

        return True
    
    def __merge(self):
        # TODO: for MILESTONE 2
        print("merge is happening")
        pass

    def __initialize_record_columns(self, rid, record_nonmeta_columns):
        record_columns = []

        # Initialize meta columns
        record_columns.append(rid) # INDIRECTION_COLUMN
        record_columns.append(rid) # RID_COLUMN
        record_columns.append(int(time())) # TIMESTAMP_COLUMN
        record_columns.append(0) # SCHEMA_ENCODING_COLUMN, starts as bit list of 0s

        # Initialize non-meta columns
        record_columns.extend(record_nonmeta_columns)

        return list(record_columns) # Return new list (not reference)
 
    # Iterate through the page ranges to find one with space
    # Returns page_range_index if successful, -1 otherwise
    def find_free_page_range(self):
        for page_range_index, page_range in enumerate(self.page_ranges):
            if page_range.has_capacity():
                return page_range_index
        return -1
    
    # Gets location of record from page directory
    # Accesses columns physical page and gets value
    # Used for index.py
    # FOR BASE RECORDS
    def get_column_value_nonmeta(self, rid, nonmeta_column_index):
        column_index = nonmeta_column_index + Config.NUM_META_COLUMNS
        
        page_range_index, logical_page_index, offset_index = self.page_directory[rid]
        return self.page_ranges[page_range_index].base_pages[logical_page_index].physical_pages[column_index].read(offset_index)

    def allocate_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def get_next_lineage_rid(self, rid):
        if rid not in self.page_directory:
            return rid
        page_range_index, tail_page_index, offset_index = self.page_directory[rid]
        next_rid = self.page_ranges[page_range_index].tail_pages[tail_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        return next_rid
    