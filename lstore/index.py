"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree
# from sortedcontainers import SortedDict
# index = SortedDict()
import pickle #rename index.py to .pickle?
# print(dir(BTrees))
class Index:

    def __init__(self, table):
        self.indices = [OOBTree() for _ in range(table.num_columns)] 
        self.table = table
    

    """
    # Returns the rid of the record of the given key value. Returns -1 if not found.
    """
    def key_to_rid(self, key_column, key_value):
        rid_list = self.table.index.locate(key_column, key_value)
        if not rid_list:
            return -1
        return rid_list[0]
       

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        rid_list = []
        
        if self.indices[column] is not None and value in self.indices[column]:
            rid_list.extend(self.indices[column][value])
            # for rid in self.indices[column][value]:
            #     rid_list.append(self.indices[column][value])
        
        return rid_list
        

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

    def create_index(self, rid, columns):
        for column_index, column_value in enumerate(columns):
            if column_value not in self.indices[column_index]: # Creates new entries for values that aren't in the index
                self.indices[column_index][column_value] = []
            self.indices[column_index][column_value].append(rid)
            
        
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        self.indices[column].clear()


    def write_index_to_disk(self):
        with open(f"{self.table.name}_index.pkl", "wb") as f:
            pickle.dump(self.indices,f)
        

    def load_index_from_disk(self):
        try:
            with open(f"{self.table.name}_index.pkl", "rb") as f:
                self.indices = pickle.load(f)
        except FileNotFoundError:
            print("NO EXISTING INDEX CREATE NEW ONE")