from lstore.config import Config
from collections import OrderedDict
from lstore.logical_page import LogicalPage
"""
There is 1 bufferpool per db, which means it's shared between tables

TODO
- Each frame stores logical page instead of physical page
- Make sure bufferpool can be used for multiple tables at a time (use table_name)
- params: know which logical page based on: table_name, page_range_index, record_type, logical_page_index
"""

class Bufferpool():
    def __init__(self, disk):
        self.disk = disk
        self.page_table = OrderedDict()
        self.frames = {}
        self.num_pinned = 0
        # self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
    
    def request_logical_page(self, table_name, page_range_index, record_type, logical_page_index):
        """ Returns a page if its in memory, will load from disk if not """
        
        page_id = (table_name, page_range_index, record_type, logical_page_index)

        if page_id in self.page_table:
            self.page_table.move_to_end(page_id) # Maintains LRU
            return self.frames[page_id].logical_page
        
        if len(self.frames) >= Config.NUM_FRAMES: # Checks if bufferpool is full
            self.evict_page()
            return self.load_page_from_disk(table_name,page_range_index, record_type, logical_page_index)
        
        return self.load_page_from_disk(table_name,page_range_index, record_type, logical_page_index)
       
    def evict_page(self):
        """ Removes LRU logical page from memory """

        if self.num_pinned == Config.NUM_FRAMES:
            raise RuntimeError("ALL FRAMES PINNED CANNOT EVICT")

        for page_id, frame in self.page_table.items(): #Find unpinned pages
            if not frame.pinned:
                self.page_table.pop(page_id)
                self.frames.pop(page_id)

                if frame.dirty:
                    table_name,page_range_index, record_type, logical_page_index = page_id
                    self.write_back_page(table_name, page_range_index, record_type, logical_page_index)
            
                return
            
    def load_page_from_disk(self, table_name, page_range_index, logical_page_index):
        """ Loads page from disk into memory. """
        logical_page = self.disk.read_logical_page(table_name,page_range_index,logical_page_index)        
        
        if logical_page is None: # Creates new logical page if DNE
            logical_page = LogicalPage(Config.NUM_META_COLUMNS)

        frame = Frame(logical_page)
        page_id = (table_name,page_range_index,logical_page_index)
        self.frames[page_id] = frame
        self.page_table[page_id] = frame  # Add to LRU tracking
        return frame.logical_page

    def write_back_page(self, table_name, page_range_index,record_type, logical_page_index):
        """ writes dirty page back to disk. """
        page_id = (table_name,page_range_index, record_type, logical_page_index)

        if page_id in self.frames:
            logical_page = self.frames[page_id].logical_page
            self.disk.write_logical_pages(table_name,page_range_index,record_type,logical_page_index,logical_page)
            self.frames[page_id].dirty = False #clean
      
    # Write back all dirty pages (called when closing the db and saving to disk)
    def write_back_all_dirty_pages(self):
        for page_id, frame in self.frames.items(): 
            if frame.dirty: 
                table_name, page_range_index,record_type, logical_page_index = page_id
                self.disk.write_logical_pages(table_name,page_range_index,record_type,logical_page_index,frame.logical_page) #uses stored location modifying the logical page back to disk
                frame.dirty = False

    def pin_page(self, table_name, page_range_index,record_type, logical_page_index):
        """ Prevents logical page from being evicted """
        page_id = (table_name,page_range_index, record_type, logical_page_index)
        if page_id in self.frames:
            if self.num_pinned >= Config.NUM_FRAMES:
                raise RuntimeError("MAXED PINS. CANNOT PIN MORE")
            
            if not self.frames[page_id].pinned:
                self.frames[page_id].pinned = True
                self.num_pinned += 1

    def unpin_page(self, table_name, page_range_index,record_type, logical_page_index):
        """ allows logical page to be evicted"""
        page_id = (table_name,page_range_index, record_type, logical_page_index)
        if page_id in self.frames:
            if self.frames[page_id].pinned:
                self.frames[page_id].pinned = False
                self.num_pinned -= 1

    def unpin_all_pages(self): # for deadlocking 
        for frame in self.frames.values():
            frame.pinned = False

# References a logical page
class Frame():
    def __init__(self, logical_page):
        #self.physical_page = physical_page # Holds the in-memory physical page
        self.dirty = False
        self.pinned = False
        self.location = () # (table_name, page_range_index, record_type, logical_page_index)
        self.logical_page = logical_page # Holds the in-memory logical page
