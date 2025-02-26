


# Milestone 2

2 threads:
- Main thread (what the user runs)
- Merging thread (starts when initializing table.py)


TODOS
- Disk saving/loading and disk functions
- Bufferpool functions for reading/writing to disk (use disk functions)
    - Make sure table.py and page_range.py is refactored to work with the bufferpool (no more direct access, must go through bufferpool)
- Merge thread


## Disk Model
SERIALIZE ALL JSONS (headers and page_directory)
SERIALIZE index

File structure:
- DB dir (contains tables) (dir name is in the path param)
    - table dir (dir name is table.name)
        - header.pkl (stores table.py's name, key, num_columns, next_rid) (serialized json)
        - page_directory.pkl (serialized python dict json)
        - index.pkl (serialized index obj)
        - page_ranges dir (contains page ranges)
            - 0 dir (page range 0)
                - header.pkl (stores page_range.py's table_name, page_range_index, num_columns, num_base_records, num_updates) (serialized json)
                - base_pages dir (contains base pages)
                    - 0 dir (base page 0)
                        - header.pkl (stores logical_page's num_columns, num_records) (serialized json)
                        - physical_pages dir (contains physical pages)
                            - 0 dir (physical page 0)
                                - header.pkl (stores physical_page's num_records) (serialized json)
                                - physical_page.data (stores physical_page's data bytearray)
                            - 1 dir (physical page 1)...etc
                    - 1 dir (base page 1)...etc
                - tail_pages dir (same format as base pages dir)
            - 1 dir (page range 1)...etc
    - table dir (with different name)...etc


Must be able to create table, write to disk, close program, open program, read from disk, and fully initialize everything in table.py

The db.py functions:
- db.open()
    - Check the given path and read the db dir, create it if it doesn't exist
    - Make sure the existing tables are read properly
- db.close()
    - Write everything to the db dir (save everything onto disk)
- db.create_table()
    - Creates the table dir with its column files
- db.drop_table()
    - Removes the table dir and its contents

a disk class can be defined in db.py:
read_db()
- read existing db
read_page(record_type, rid)
- use rid to correctly locate page's location in disk, return physical_page
write_page(record_type, rid, physical_page)
- use rid to correctly locate page's location in disk, write the physical_page


## Bufferpool
The bufferpool has Config.NUM_FRAMES frames, each frame containing a logical page
Bufferpool is initialized in db.open()

<!-- bufferpool.py called by table.py's read/write functions to manage data between memory and disk -->

must expose request_page(rid) for loading a physical from disk to memory
Also expose the following for pinning a frame/page:
- start_transaction(rid)
- finish_transaction(rid)

must translate rid into the record's physical page location vars

request_page(), Is page in pool?
- Yes: acquire read lock, return frame's page
- No: bring_page_into_pool(), is frame empty?
    - Yes: get_page_from_disk(), acquire read lock, return frame's page
    - No: evict existing frame based on replacement policy, replace it with desired page

Replacement policy: Least-recently-used (LRU), don't evict pinned frames

Frame
- Each frame holds a logical page
    - logical page identified by its page_range_index, record_type, logical_page_index
- State: Can be full, dirty, or empty
- Can be pinned or not

Write dirty frames to disk

Useful slides: https://expolab.org/ecs165a-winter2024/top_presentations/M2/cowabungadb.pdf


## Merging
Runs in its own thread, initialized in table.py
table.py will need to keep initialize and increment the var num_updates

Thread waiting
- Merge thread will conditional wait (waits until a cond var is signaled) inside a while loop that checks if it should continue waiting or merge.
    - while len(self.merge_queue) == 0
- Signal the cond var when appending the merge queue.
- Once the merge thread escapes the cond wait, reset num_updates to 0 and do the merge

Merging process
- Copy of all base pages' records
- Copy of all tail pages' records
- For each base record: Get the latest record
    - Use query.py's get_record_list_lineage with version=0
- With the latest records, update the existing base records in the base pages
- With copy of tail records, use their rids to delete them from the page range
    - No need to actually delete the tail records from memory, just mark them as deleted
- Write the base records (which are updated) to disk


As for merge:
Modify the update record func, from now on we have to add 2 tail records instead of just 1.
The first tail record inserted is called a snapshot record, which stores the same data columns as the base record's current latest.
It's schema encoding column should be 0 so we can identify it (since it didn't update anything).

The next tail record (whose indirection points to that snapshot record since its another tail
record) is the actual update and we do that normally. This means we'll have double the tail records,
with snapshot tail records layered in-between. Due to the new snapshot tail records, we'll need to modify
 the get_next_lineage function which should skip the snapshot record (identified by schema encoding of 0).

We can add a bool param to specify if we should skip the snapshots, which defaults to true, so we can still
use it to get next lineage without skipping the snapshot (important for merging)

For the merge, we start by accessing the base record's latest snapshot record (go through indirection 2 times)
and storing it in a newly allocated base record. We don't update the base_rid in the page directory.
The base rid should still point to its base record. This new base record is simply used as working memory
and to persist the updated data incase the database is closed during the merge.

We'll only be merging into the base record's data columns, we should not touch the metadata. This way, new updates can still update
the base record's indirection. After the merge, we can delete the newly allocated base record. TA didn't say this,
but we could have just 1 base record dedicated to merging, to prevent creating a new base record for each time we merge a base record.

In the end, the base record is still in the same location, just that its data columns are updated now.
I'm waiting for an announcement from the TA on whether select/sum version should access the merged tail
records or go straight to the base record. TPS plays a role here. Based on that, we'll possibly have to modify the querying to use TPS.





# Milestone 1

Database
- Table[] size: infinite

Table
- page_range[] size: infinite

page_range
- base_page[16]
- tail_page[] size: infinite

logical_page (base_page or tail_page)
- physical_page[user_cols + meta_cols]

physical_page (4096 bytes)
- 512 records of 8 bytes

Buffer pool Management
- create pages 
- track pages in memory

- page_directory & buffer pool files 

    page_directory: 

    buffer_pool

