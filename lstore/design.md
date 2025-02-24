


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
File structure:
- DB dir (contains tables) (dir name is in the path param)
    - table dir (dir name is table.name)
        - table.hdr (stores table.py's name, key, num_columns, next_rid)
        - page_directory.json
        - index.pickle (use pickle to serialize it)
        - page_ranges dir (contains page ranges)
            - 0 dir (page range 0)
                - page_range.hdr (stores page_range.py's num_base_records, num_tail_pages, num_updates)
                - base_pages dir (contains base pages)
                    - 0 dir (base page 0)
                        - base_page.hdr (stores logical_page's num_records)
                        - physical_pages dir (contains physical pages)
                            - 0 dir (physical page 0)
                                - physical_page.hdr (stores physical_page's num_records)
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

