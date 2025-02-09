"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        self.indices = [OOBTree() for _ in range(table.num_columns)] 
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value): 
        if self.indices[column] is not None and value in self.indices[column]: 
            rid = self.indices[column][value]
            return rid
        
        return -1   
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rid_list = []
        for value, rid in self.indices[column].items(begin, end): # Within the column it accesses all values based on the given range 
            rid_list.extend(rid)

        return rid_list




    """
    # optional: Creates index on target column in the table. Searches for all RIDs mapping it's column values to their each RID later storing them 
    """

    def create_index(self, columns):
        for column in columns:
            for rid in self.table.page_directory:
                value = self.table.get_column_value(rid,column) 
                if value not in self.indices[column]: # Creates new entries for values that aren't in the index
                    self.indices[column][value]=[]
                self.indices[column][value].append(rid)  
            
            
        
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        self.indices[column].clear()
