
class PhysicalPage:

    PAGE_SIZE = 4096  # 4KB
    RECORD_SIZE = 8    # 64-bit integers (8 bytes)
    MAX_RECORDS = PAGE_SIZE // RECORD_SIZE  # Number of records allowed per page

    def __init__(self):
        self.num_records = 0 # Initializes number of records stored
        self.data = bytearray(self.PAGE_SIZE) # Sets page size

    def has_capacity(self):
        return self.num_records < self.MAX_RECORDS
    
    def read(self, index):
        if index >= self.num_records | index < 0:
            raise IndexError("Index is out of bounds, please enter a valid index.")
        
        offset = index * self.RECORD_SIZE
        return int.from_bytes(self.data[offset:offset + self.RECORD_SIZE], byteorder='big')

    def create(self, value):
        if not self.has_capacity(): # Checks if there is room in page for new record to be written
            raise Exception("Cannot write, this physical page is full.")

        offset = self.num_records * self.RECORD_SIZE # Compute offset and insert value
        self.update(self, offset, value)
        self.num_records += 1 # Increments number of records stored for each record written

        return offset

    def update(self, index, value):
        if not (-2**63 <= value < 2**63): # Checks to see if value lies outside size of 64 bit integer
            raise ValueError("Value is larger than 64 bit int can store.")
        
        value_bytes = value.to_bytes(self.RECORD_SIZE, byteorder='big') # Convert value to bytes (big-endian format)
        
        self.data[index : (index + self.RECORD_SIZE)] = value_bytes

        return index
    
    # TODO: Delete function for when a merge occurs. Fix other functions to accommodate this
    def delete(self, index):
        pass