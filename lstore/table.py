from lstore.index import Index
from lstore.physical_page import PhysicalPage
from lstore.config import Config
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from time import time

# from BTrees.OOBTree import BTree


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

        self.key = key + Config.NUM_META_COLUMNS

        self.num_columns = num_columns + Config.NUM_META_COLUMNS
        
        # Maps RID -> (page_range_index, logical_page-index, offset_index)
        self.page_directory = {}

        self.index = Index(self)
        
        self.next_rid = 1 # RID of 0 is reserved to indicate deletion

        self.page_ranges = [PageRange(self.num_columns)]

    # Reads base record (includes meta-columns)
    # rid starts at 1 as defined above
    def read_record(self, rid):
        if rid not in self.page_directory:
            return False # Invalid base record rid
    
        page_range_index, logical_page_index, offset_index = self.page_directory[rid]
        record = self.page_ranges[page_range_index].read_record(Config.BASE_RECORD, logical_page_index, offset_index)

        # record_metadata = record[:4]
        # record_data = record[4:]
        
        return record

    # Creates NEW base record
    def create_record(self, record_nonmeta_columns):
        if len(record_nonmeta_columns) != (self.num_columns - Config.NUM_META_COLUMNS):
            return False # Record could not be created because it does not match the number of non-meta columns in the table

        page_range_index = self.find_free_page_range()
        if page_range_index < 0:
            # Allocate new page range
            self.page_ranges.append(PageRange(self.num_columns))
            page_range_index = len(self.page_ranges) - 1

        # Get unique RID
        rid = self.next_rid
        self.next_rid += 1
        
        record_columns = self.__initialize_record_columns(rid, record_nonmeta_columns)

        # Create record and add to page directory
        logical_page_index, offset_index = self.page_ranges[page_range_index].create_record(Config.BASE_RECORD, record_columns)
        self.page_directory[rid] = (page_range_index, logical_page_index, offset_index)

        return True # Record was created

    # Update a record's column value (either creates new tail record or updates existing tail record)
    # nonmeta_column_index shouldn't account for the meta columns (0 for first non-meta column)
    def update_record(self, base_rid, nonmeta_column_index, column_value):
        if base_rid not in self.page_directory:
            return False # Invalid base record rid
    
        column_index = nonmeta_column_index + Config.NUM_META_COLUMNS

        # Check if the base record has a tail record that already exists
        page_range_index, base_page_index, offset_index = self.page_directory[base_rid]
        tail_rid = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        if tail_rid != base_rid and tail_rid in self.page_directory:
            # Update existing tail record
            page_range_index, tail_page_index, offset_index = self.page_directory[tail_rid]
            self.page_ranges[page_range_index].update_tail_record_value(tail_page_index, offset_index, column_index, column_value)
        
        # Create new tail record
        else:
            tail_rid = self.next_rid
            self.next_rid += 1

            # Directly update base record's indirection column to point to new tail record
            self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].update_value(offset_index, tail_rid)

            # Create new tail record with given column value
            record_nonmeta_columns = [0] * (self.num_columns - Config.NUM_META_COLUMNS)
            record_nonmeta_columns[nonmeta_column_index] = column_value
            record_columns = self.__initialize_record_columns(tail_rid, record_nonmeta_columns)

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
        print("merge is happening")
        pass

    def __initialize_record_columns(self, rid, record_nonmeta_columns):
        record_columns = []

        # Initialize meta columns
        record_columns.append(rid) # INDIRECTION_COLUMN
        record_columns.append(rid) # RID_COLUMN
        record_columns.append(int(time())) # TIMESTAMP_COLUMN
        record_columns.append(0) # SCHEMA_ENCODING_COLUMN

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
    def get_column_value(self, rid, column):
        page_range_index, logical_page_index, offset_index = self.page_directory[rid]
        return self.page_ranges[page_range_index].read_column(logical_page_index, column, offset_index)
