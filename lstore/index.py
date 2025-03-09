"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree
import pickle 

class Index:

    def __init__(self, table):
        self.table = table # Exclude when serializing
        self.indices = [OOBTree() for _ in range(self.table.num_columns)]

    """
    # Returns the rid of the record of the given key value. Returns -1 if not found.
    """
    def key_to_rid(self, key_column, key_value):
        rid_list = self.locate(key_column, key_value)
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

    def create_index_with_rid(self, rid, columns):
        for column_index, column_value in enumerate(columns):
            if column_value not in self.indices[column_index]: # Creates new entries for values that aren't in the index
                self.indices[column_index][column_value] = []
            self.indices[column_index][column_value].append(rid)
    
    def delete_index_entry(self, rid, columns):
        for column_index, column_value in enumerate(columns):
            if column_value in self.indices[column_index]: # Deletes entries for values that is in the index
                del self.indices[column_index][column_value]


    def create_index(self, column):
        """ Creates an index for a specific column if it doesn't exist. """
        return
        # if self.indices[column] is None:
        #     self.indices[column] = OOBTree()

        # # Populate the index with existing records
        # for rid, page_info in self.table.page_directory.items():
        #     value = self.table.get_column_value_nonmeta(rid, column)  # gets column value

        #     if value not in self.indices[column]:
        #         self.indices[column][value] = set()  # initialize set for RIDs
            
        #     self.indices[column][value].append(rid)  # add rid to index
            
        
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        return
        # self.indices[column].clear()

    # ----------------------------------------------------------------

    def __getstate__(self):
        # Create a copy of the instance's dictionary (attributes)
        state = self.__dict__.copy()
        if 'table' in state:
            del state['table']  # Exclude the table reference from being serialized
        return state

    def __setstate__(self, state):
        # Restore the state and set table to None (or handle as needed)
        self.__dict__.update(state)
        self.table = None  # Ensure table is None after deserialization


 #################

    # def write_index_to_disk(self):
    #     with open(f"{self.table.name}_index.pkl", "wb") as f:
    #         pickle.dump(self.indices,f)
        

    # def load_index_from_disk(self):
    #     try:
    #         with open(f"{self.table.name}_index.pkl", "rb") as f:
    #             self.indices = pickle.load(f)
    #     except FileNotFoundError:
    #         print("NO EXISTING INDEX CREATE NEW ONE")