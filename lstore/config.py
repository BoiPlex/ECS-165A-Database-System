# Singleton class that stores object that acts as centralized storage for all config options and constant values used in the code

class Config:
    MAX_RECORDS_PER_LOGICAL_PAGE = 512
    MAX_RECORDS_PER_PAGE_RANGE = MAX_RECORDS_PER_LOGICAL_PAGE * 16 # = 8192 records
    MAX_PAGE_RANGE_SIZE = MAX_RECORDS_PER_PAGE_RANGE * 8 # = 65536 bytes

    NUM_BASE_PAGES = 16

    # Metadata columns
    NUM_META_COLUMNS = 5
    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3
    TAIL_PAGE_SEQUENCE_COLUMN = 4

    # Used to pick record type (base or tail)
    BASE_RECORD = 0
    TAIL_RECORD = 1

    # Bufferpool
    NUM_FRAMES = 500

    # Merging
    NUM_UPDATES_FOR_MERGE = 100000

    # Lock types
    LOCK_TYPE_SHARED = 0
    LOCK_TYPE_EXCLUSIVE = 1

"""
Indirection col: Points to tail record change (when creating, points to itself and should be same as RID. When updated (record update created in tail page), should point to that new record)
- Base record points to itself (on create)
- Tail record points to itself (on create)
- Update base record when tail record is created

RID col
    - Stores record ID

Timestamp col
    - Stores timestamp of latest change

Schema encoding col
    - "Each base record also contains a schema encoding column. This is a bit vector with one
        bit per column that stores information about the updated state of each column. In the
        base records, the schema encoding will include a 0 bit for all the columns that have not
        yet been updated and a 1 bit for those that have been updated. This helps optimize
        queries by determining whether we need to follow the lineage or not. In non-cumulative
        tail records, the schema encoding serves to distinguish between columns with updated
        values and those with NULL values"
- Initialize to 0

Tail Page Sequence Number Col
    - "The lineage of each base page is
    maintained internally using the notion of Tail-Page Sequence Number (TPS). The TPS
    is lineage information that is kept on every base page. TPS tracks the RID of the last tail
    record (TID) that has been applied (merged) to a base page. The TID space could be
    drawn from the range 2 to , decreasing monotonically."
"""