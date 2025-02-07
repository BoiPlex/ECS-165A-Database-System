class PhysicalPage:

    PAGE_SIZE = 4096  # 4KB
    RECORD_SIZE = 8    # 64-bit integers (8 bytes)
    MAX_RECORDS = PAGE_SIZE // RECORD_SIZE  # Number of records allowed per page

    def __init__(self):
        self.num_records = 0 # Initializes number of records stored
        self.data = bytearray(self.PAGE_SIZE) # Sets page size

    # def has_capacity(self):
    #     return self.num_records < self.MAX_RECORDS
    
    def read(self, offset_index):
        if offset_index >= self.num_records | offset_index < 0:
            raise IndexError("Index is out of bounds, please enter a valid index.")
        
        offset = offset_index * self.RECORD_SIZE
        return int.from_bytes(self.data[offset: (offset + self.RECORD_SIZE)], byteorder='big')

    def create(self, value):
        offset_index = self.num_records * self.RECORD_SIZE # Compute offset and insert value
        
        self.update_value(self.num_records, value) # Can raise error

        self.num_records += 1 # Increments number of records stored for each record written
        return offset_index

    def update_value(self, offset_index, value):
        if not (-2**63 <= value < 2**63): # Checks to see if value lies outside size of 64 bit integer
            raise ValueError("Value is larger than 64 bit int can store.")

        # self.data[offset_index : (offset_index + self.RECORD_SIZE)] = bytearray(self.RECORD_SIZE) 
        start_index = offset_index*self.RECORD_SIZE
        value_bytes = value.to_bytes(self.RECORD_SIZE, byteorder='big') # Convert value to bytes (big-endian format)
        self.data[start_index : (start_index + self.RECORD_SIZE)] = value_bytes
        
    
    # TODO: MILESTONE 2: Delete function for when a merge occurs. Fix other functions to accommodate this
    def delete(self, offset_index):
        pass





    # def create(self, value):
    #     if not self.has_capacity(): # Checks if there is room in page for new record to be written
    #         raise Exception("Cannot write, this physical page is full.")

    #     offset = self.num_records * self.RECORD_SIZE # Compute offset and insert value
    #     self.update(offset, value)
    #     self.num_records += 1 # Increments number of records stored for each record written

    #     return offset