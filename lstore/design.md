


# Milestone 2

2 threads:
- Main thread (what the user runs)
- Merging thread (starts when initializing table.py)

## Disk
File structure:
- DB dir (contains tables) (dir name is in the path param)
    - table 0 dir (contains table.columns columns) (dir name is table.name)
        - column 0 file
        - column 1 file
        - etc

The 4 meta-columns aren't stored in disk

The db.py functions:
- db.open()
    - Check the given path and read the db dir, create it if it doesn't exist
- db.close()
    - Write everything to the db dir (save everything onto disk)
- db.create_table()
    - Creates the table dir with its column files
- db.drop_table()
    - Removes the table dir and its contents

Column file
- contains a list of physical pages
    - each physical page is 4096 KB, with its position defined by disk_page_index
    - you can read/write a physical page by using disk_page_index
        - use a dictionary to map rid to disk_page_index
    - remember there are 16 physical pages per page range
    -   write the 16 physical pages every time a page range is created

disk read/writes and the dictionary can be defined in db.py


## Bufferpool
The bufferpool has Config.NUM_FRAMES frames
Initialized in db.open()

Frame
- Each frame references a physical page
- State: Can be full, dirty, or empty
- Contains pages

Replacement policy: Least-recently-used (LRU)
- Keep track of 

Write dirty frames to disk


## Merging
Runs in its own thread, initialized in table.py
table.py will need to keep initialize and increment the var num_updates

Thread waiting
- Merge thread will conditional wait (waits until a cond var is signaled) inside a while loop that checks if it's ok to merge.
    - while num_updates < Config.NUM_UPDATES_FOR_MERGE
- Signal the cond var every time the num of updates var is incremented.
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

