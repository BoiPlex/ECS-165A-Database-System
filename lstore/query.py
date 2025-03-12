from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import Config


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table: Table = table
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.table.index.key_to_rid(self.table.key, primary_key)

        if rid < 1: # Can't delete record that's already marked for deletion (rid of 0)
            return False
        
        return self.table.delete_record(rid)
            
    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        return self.table.create_record(columns)

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        base_rid = self.table.index.key_to_rid(self.table.key, primary_key)
        
        success = self.table.update_record(base_rid, columns)
        if not success:
            return False
        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retrieve.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key) # base rids
        
        record_list = [self.table.read_record(rid) for rid in rid_list]        
        record_list = self.get_record_list_lineage(record_list, relative_version)
        
        return self.filter_by_projected_columns(record_list, projected_columns_index)
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retrieve.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(rid_list) == 0:
            return False

        record_list = []
        for rid in rid_list:
            record = self.table.read_record(rid)
            record_list.append(record)
        
        record_list = self.get_record_list_lineage(record_list, relative_version)
        if record_list is False:
            return False
        
        sum_value = 0
        for record in record_list:
            sum_value += record.columns[aggregate_column_index]
            
        return sum_value

    
    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        # Skip?
        # r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        # if r is not False:
        #     updated_columns = [None] * self.table.num_columns
        #     updated_columns[column] = r[column] + 1
        #     u = self.update(key, *updated_columns)
        #     return u
        # return False
        pass

    def get_record_list_lineage(self, record_list, relative_version):
        new_record_list = list(record_list)

        for i, record in enumerate(record_list):
            base_rid = record.rid
            current_version = 0
            current_rid = self.table.get_next_lineage_rid(Config.BASE_RECORD, record.rid) # Latest version (version 0)

            while current_version > relative_version and current_rid != base_rid:
                current_rid = self.table.get_next_lineage_rid(Config.TAIL_RECORD, current_rid)
                current_version -= 1

            record_type = Config.BASE_RECORD if current_rid == base_rid else Config.TAIL_RECORD
            new_record_list[i] = self.table.read_record(current_rid)

        return new_record_list


    def filter_by_projected_columns(self, record_list, projected_columns_index):
        for record in record_list:
            columns_result = []
            for column_index, should_return_column in enumerate(projected_columns_index):
                if should_return_column == 1:
                    columns_result.append(record.columns[column_index])
            record.columns = columns_result
        return record_list


    """
    CUSTOM METHOD: MAX (aggregation)
    """
    def max(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(rid_list) == 0:
            return False

        max_value = 0
        for rid in rid_list:
            value = self.table.get_column_value_nonmeta(rid, aggregate_column_index)
            max_value = max(value, max_value)
            
        return max_value

    """
    CUSTOM METHOD: MIN (aggregation)
    """
    def min(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(rid_list) == 0:
            return False

        min_value = 0
        for rid in rid_list:
            cur_value = self.table.get_column_value_nonmeta(rid, aggregate_column_index)
            min_value = min(cur_value, min_value)
            
        return min_value
    
    """
    CUSTOM METHOD: COUNT (aggregation)
    """
    def count(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        return len(rid_list)

    """
    CUSTOM METHOD: AVG (aggregation)
    """
    def avg(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(rid_list) == 0:
            return False

        total_value = 0
        for rid in rid_list:
            total_value += self.table.get_column_value_nonmeta(rid, aggregate_column_index)
            
        return total_value / len(rid_list)
