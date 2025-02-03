# Singleton class that stores object that acts as centralized storage for all config options and constant values used in the code

class Config:
    MAX_RECORDS_PER_LOGICAL_PAGE = 512
    MAX_RECORDS_PER_PAGE_RANGE = MAX_RECORDS_PER_LOGICAL_PAGE * 16 # = 8192 records
    MAX_PAGE_RANGE_SIZE = MAX_RECORDS_PER_PAGE_RANGE * 8 # = 65536 bytes

    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3

    NUM_META_COLUMNS = 4