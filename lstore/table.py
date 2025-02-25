from lstore.index import Index
from lstore.physical_page import PhysicalPage
from lstore.config import Config
from lstore.page_range import PageRange
from lstore.logical_page import LogicalPage
from lstore.bufferpool import BufferPool

from time import time
import threading
import copy

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns:
    int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key # Key column index (doesn't include meta-columns)
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        
        # Maps RID -> (page_range_index, record_type, logical_page-index, offset_index)
        self.page_directory = {}

        self.index = Index(self)
        
        self.next_rid = 1 # RID of 0 is reserved to indicate deletion

        self.page_ranges = [PageRange(num_columns + Config.NUM_META_COLUMNS, self.bufferpool)]

        # Merging
        self.merge_queue = []
        self.merge_lock = threading.Lock()
        self.merge_cond = threading.Condition(self.merge_lock)
        merge_thread = threading.Thread(target=self.__merge, daemon=True)
        merge_thread.start()

    # Reads base/tail record (includes meta-columns)
    # rid starts at 1 as defined above
    def read_record(self, record_type, rid, include_metacolumns=False):
        if rid not in self.page_directory:
            return False # Invalid base record rid
    
        page_range_index, _, logical_page_index, offset_index = self.page_directory[rid]

        logical_page_frame = self.bufferpool.request_logical_page_frame(self.name, page_range_index, record_type, logical_page_index)

        record_columns = logical_page_frame.read_record(offset_index)

        record_metadata = record_columns[:4]
        record_data = record_columns[4:]
        
        if include_metacolumns:
            record = Record(rid, record_columns[self.key], record_metadata + record_data)
        else:
            record = Record(rid, record_columns[self.key], record_data)
        
        return record

    # Creates NEW base record (insert)
    def create_record(self, record_nonmeta_columns):
        if len(record_nonmeta_columns) != self.num_columns or self.index.key_to_rid(self.key, record_nonmeta_columns[self.key]) != -1:
            return False # Record could not be created

        page_range_index = self.find_free_page_range()
        if page_range_index < 0:
            # Allocate new page range
            self.page_ranges.append(PageRange(self.num_columns + Config.NUM_META_COLUMNS, self.bufferpool))
            page_range_index = len(self.page_ranges) - 1
        
        # Get unique RID
        base_rid = self.allocate_rid()
        
        record_columns = self.__initialize_record_columns(base_rid, record_nonmeta_columns)

        # Create record and add to page directory
        logical_page_index, offset_index = self.page_ranges[page_range_index].create_record(Config.BASE_RECORD, record_columns)
        self.page_directory[base_rid] = (page_range_index, Config.BASE_RECORD, logical_page_index, offset_index)

        # Add to index
        self.index.create_index(base_rid, record_nonmeta_columns)

        return True # Record was created

    # Update a record's column value (either creates new tail record or updates existing tail record)
    # nonmeta_column_index shouldn't account for the meta columns (0 for first non-meta column)
    def update_record(self, base_rid, record_nonmeta_columns):
        if base_rid not in self.page_directory or len(record_nonmeta_columns) != self.num_columns or self.index.key_to_rid(self.key, record_nonmeta_columns[self.key]) != -1:
            return False

        # Base record's indexes
        page_range_index, record_type, base_page_index, offset_index = self.page_directory[base_rid]

        schema_encoding_column_value = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(offset_index)

        # Update base record's schema encoding column
        for nonmeta_column_index, column_value in enumerate(record_nonmeta_columns):
            if column_value == None:
                continue
            bit_column_index = self.num_columns - nonmeta_column_index - 1
            schema_encoding_column_value |= (1 << bit_column_index)
        self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].update_value(offset_index, schema_encoding_column_value)

        # Get latest existing record
        latest_rid = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        latest_record_type = Config.BASE_RECORD if latest_rid == base_rid else Config.TAIL_RECORD
        latest_record_copy =  self.read_record(latest_record_type, latest_rid)

        'Creates snapshot record'
        # Get unique RID for the snapshot tail RID
        snapshot_tail_rid = self.allocate_rid()

        snapshot_nonmeta_columns = latest_record_copy.columns
        for nonmeta_column_index, column_value in enumerate(record_nonmeta_columns):
            if column_value is None:
                continue
            snapshot_nonmeta_columns[nonmeta_column_index] = column_value

        # Initialize the columns and set the indirection to previous record
        record_columns = self.__initialize_record_columns(snapshot_tail_rid, snapshot_nonmeta_columns)
        record_columns[Config.INDIRECTION_COLUMN] = latest_rid
        
        # Create new tail record and update page directory
        logical_page_index, offset_index = self.page_ranges[page_range_index].create_record(Config.TAIL_RECORD, latest_record_copy.columns)
        self.page_directory[snapshot_tail_rid] = (page_range_index, Config.TAIL_RECORD, logical_page_index, offset_index)

        self.page_ranges[page_range_index].num_updates += 1

        'Creates full update record'
        # Allocate RID for latest tail record
        tail_rid = self.allocate_rid()
        
        # Update base record's indirection column
        self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].update_value(offset_index, tail_rid) 

        # Get latest existing record's data and update its column
        new_nonmeta_columns = latest_record_copy.columns
        for nonmeta_column_index, column_value in enumerate(record_nonmeta_columns):
            if column_value is None:
                continue
            new_nonmeta_columns[nonmeta_column_index] = column_value
        
        # Initialize the columns and set the indirection to previous record
        record_columns = self.__initialize_record_columns(tail_rid, new_nonmeta_columns)
        record_columns[Config.INDIRECTION_COLUMN] = snapshot_tail_rid

        # Create new tail record and update page directory
        logical_page_index, offset_index = self.page_ranges[page_range_index].create_record(Config.TAIL_RECORD, record_columns)
        self.page_directory[tail_rid] = (page_range_index, Config.TAIL_RECORD, logical_page_index, offset_index)


        # Update page range's num_updates, if need to merge then append to the self.merge_queue and notify the merge thread
        self.page_ranges[page_range_index].num_updates += 1
        if self.page_ranges[page_range_index].num_updates >= Config.NUM_UPDATES_FOR_MERGE:
            self.page_ranges[page_range_index].num_updates = 0
            self.merge_queue.append(page_range_index)
            self.merge_cond.notify()
        
        return True
    
    '''
    Deletes base record (along with associated tail record)
    For now just mark as deleted, will be fully implemented with __merge(self)
    '''
    def delete_record(self, base_rid):
        if base_rid not in self.page_directory:
            return False # Invalid rid
    
        # Delete base record
        page_range_index, record_type, base_page_index, offset_index = self.page_directory[base_rid]
        self.page_ranges[page_range_index].mark_to_delete_record(Config.BASE_RECORD, base_page_index, offset_index)
        del self.page_directory[base_rid]

        # Check if the base record has a tail record that already exists
        tail_rid = self.page_ranges[page_range_index].base_pages[base_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        if tail_rid in self.page_directory:
            # Delete associated tail record
            page_range_index, record_type, tail_page_index, offset_index = self.page_directory[tail_rid]
            self.page_ranges[page_range_index].mark_to_delete_record(Config.TAIL_RECORD, tail_page_index, offset_index)
            del self.page_directory[tail_rid]

        return True


    '''
    Merges tail record data into base records
    '''
    def __merge(self):
        # TODO: for MILESTONE 2
        while True:
            with self.merge_cond:
                while len(self.merge_queue) == 0:
                    self.merge_cond.wait()
                
                print("merge is happening")

                page_range_index = self.merge_queue.pop(0)
                page_range = self.page_ranges[page_range_index]

                # self.bufferpool.pin_page(self.name, page_range_index, Config.)

                # Get copy of base pages
                base_pages_copy = []
                for base_page_index in range(Config.NUM_BASE_PAGES):
                    base_page_frame = self.bufferpool.request_logical_page_frame(self.name, page_range_index, Config.BASE_RECORD, base_page_index)
                    
                    self.bufferpool.pin_frame(base_page_frame)
                    base_page = base_page_frame.logical_page
                    base_page_copy = copy.copy(base_page)
                    self.bufferpool.unpin_frame(base_page_frame)

                    if base_page_copy.physical_pages[Config.INDIRECTION_COLUMN] == 0:
                        continue
                    base_pages_copy.append(base_page_copy)
                
                # tail_pages_copy = []
                # for tail_page_index in range(page_range.num_tail_pages):
                #     tail_page = self.bufferpool.request_logical_page(self.name, page_range_index, Config.TAIL_RECORD, tail_page_index)
                #     tail_page_copy = copy.copy(tail_page)
                #     tail_pages_copy.append(tail_page_copy)

                # Iterates through all records, for only data columns: merge the latest snapshot tail record into base record
                for base_page_copy in base_pages_copy:
                    base_rids = base_page_copy.physical_pages[Config.RID_COLUMN].read_all()
                    for base_rid in base_rids:
                        latest_snapshot_rid = self.get_latest_snapshot_rid(base_rid)
                        latest_snapshot_record_type = Config.BASE_RECORD if latest_snapshot_rid == base_rid else Config.TAIL_RECORD
                        if latest_snapshot_record_type == Config.BASE_RECORD:
                            continue
                        else:
                            # Base record's meta-columns
                            base_record = self.read_record(Config.BASE_RECORD, base_rid, include_metacolumns=True)
                            base_meta_columns = base_record.columns[0:Config.NUM_META_COLUMNS]

                            # Latest snapshot's data columns
                            latest_snapshot_record = self.read_record(Config.TAIL_RECORD, latest_snapshot_rid, include_metacolumns=False)
                            latest_snapshot_data_columns = latest_snapshot_record.columns # New reference obj, can modify

                            # Create merged base record's columns. Don't touch the base record's meta-columns
                            new_columns = base_meta_columns + latest_snapshot_data_columns

                            # Update base record
                            _, _, base_page_index, offset_index = self.page_directory[base_rid]
                            self.bufferpool.request_logical_page_frame(self.name, page_range_index, Config.BASE_RECORD, base_page)

                        # # Allocate new page range
                        # new_page_range_index = self.find_free_page_range()
                        # if new_page_range_index < 0:
                        #     # Allocate new page range
                        #     self.page_ranges.append(PageRange(self.num_columns + Config.NUM_META_COLUMNS, self.bufferpool))
                        #     page_range_index = len(self.page_ranges) - 1
                        
                        # # Create record and allocate base page index and offset index
                        # new_base_page_index, new_offset_index = self.page_ranges[new_page_range_index].create_record(Config.BASE_RECORD, new_columns)
                        # # Update base record's page directory to point to new location
                        # self.page_directory[base_rid] = (new_page_range_index, Config.BASE_RECORD, new_base_page_index, new_offset_index)

    '''
    Readies record columns for creation/updating
    '''
    def __initialize_record_columns(self, rid, record_nonmeta_columns):
        record_columns = []

        # Initialize meta columns
        record_columns.append(rid) # INDIRECTION_COLUMN
        record_columns.append(rid) # RID_COLUMN
        record_columns.append(int(time())) # TIMESTAMP_COLUMN
        record_columns.append(0) # SCHEMA_ENCODING_COLUMN, starts as bit list of 0s
        record_columns.append(0) # TAIL_PAGE_SEQUENCE_COLUMN

        # Initialize non-meta columns
        record_columns.extend(record_nonmeta_columns)

        return list(record_columns) # Return new list (not reference)
 
    '''
    Iterate through the page ranges to find one with space
    Returns page_range_index if successful, -1 otherwise
    '''
    def find_free_page_range(self):
        for page_range_index, page_range in enumerate(self.page_ranges):
            if page_range.has_capacity():
                return page_range_index
        return -1
    
    '''
    Gets location of record from page directory
    Accesses columns physical page and gets value
    Used for index.py
    FOR BASE RECORDS
    '''
    def get_column_value_nonmeta(self, rid, nonmeta_column_index):
        column_index = nonmeta_column_index + Config.NUM_META_COLUMNS
        
        page_range_index, record_type, logical_page_index, offset_index = self.page_directory[rid]
        return self.page_ranges[page_range_index].base_pages[logical_page_index].physical_pages[column_index].read(offset_index)

    def allocate_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def get_next_lineage_rid(self, record_type, rid, skip_snapshot=True):
        if rid not in self.page_directory:
            return rid
        
        page_range_index, record_type, logical_page_index, offset_index = self.page_directory[rid]
        logical_pages = self.page_ranges[page_range_index].get_logical_pages(record_type)

        next_rid = logical_pages[logical_page_index].physical_pages[Config.INDIRECTION_COLUMN].read(offset_index)
        if skip_snapshot:
            next_page_range_index, next_record_type, next_logical_page_index, next_offset_index = self.page_directory[next_rid]
            next_logical_pages = self.page_ranges[next_page_range_index].get_logical_pages(next_record_type)
            schema_encoding = next_logical_pages[next_logical_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(next_offset_index)
            if schema_encoding == 0:
                return self.get_next_lineage_rid(record_type, next_rid, skip_snapshot=False)
            
        return next_rid
    
    '''
    Get the latest snapshot's rid for a given base record
    '''
    def get_latest_snapshot_rid(self, base_rid):
        current_rid = self.table.get_next_lineage_rid(Config.BASE_RECORD, base_rid, skip_snapshot=False) # Latest version

        while True:
            page_range_index, record_type, logical_page_index, offset_index = self.page_directory[current_rid]
            logical_pages = self.page_ranges[page_range_index].get_logical_pages(record_type)
            schema_encoding = logical_pages[logical_page_index].physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(offset_index)
            if schema_encoding == 0 or current_rid == base_rid:
                break
            current_rid = self.table.get_next_lineage_rid(Config.TAIL_RECORD, current_rid)
        
        return current_rid

    
    # def get_tail_page_sequence_rid(self, )
    