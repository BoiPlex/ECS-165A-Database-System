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
        self.frames = {} # Maps location (table_name, page_range_index, record_type, logical_page_index) to frame
        self.num_pinned = 0
        # self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
    
    def request_logical_page_frame(self, num_columns, table_name, page_range_index, record_type, logical_page_index):
        """ Returns a page if its in memory, will load from disk if not. Returns the frame object """
        
        location = (table_name, page_range_index, record_type, logical_page_index)

        if location in self.page_table:
            self.page_table.move_to_end(location) # Maintains LRU
            return self.frames[location]
        
        if len(self.frames) >= Config.NUM_FRAMES: # Checks if bufferpool is full
            self.evict_frame()
        
        frame = self.load_frame_from_disk(num_columns, table_name, page_range_index, record_type, logical_page_index)
         
        if frame.logical_page.num_records==0:
            frame.dirty=True
        return frame
       
    def load_frame_from_disk(self, num_columns, table_name, page_range_index, record_type, logical_page_index):
        """ Loads page from disk into memory. """
        logical_page = self.disk.read_logical_page(table_name, page_range_index, record_type, logical_page_index)        
        
        if logical_page is None: # Creates new logical page if DNE
            logical_page = LogicalPage(num_columns)

        frame = Frame(logical_page, (table_name, page_range_index, record_type, logical_page_index))
        #page_id = (table_name,page_range_index,logical_page_index)
        self.frames[frame.location] = frame
        self.page_table[frame.location] = frame  # Add to LRU tracking
        return frame

    def evict_frame(self):
        """ Removes LRU logical page from memory """

        if self.num_pinned == Config.NUM_FRAMES:
            raise RuntimeError("ALL FRAMES PINNED CANNOT EVICT")

        for location, frame in self.page_table.items(): #Find unpinned pages
            if not frame.pinned:
                if frame.dirty:
                    self.write_back_frame(frame)

                self.page_table.pop(location)
                self.frames.pop(location)
                return

    def write_back_frame(self, frame):
        """ writes dirty page back to disk. """
        if not frame.dirty:
            return
        
        table_name, page_range_index,record_type, logical_page_index= frame.location
        # page_id = (table_name,page_range_index, record_type, logical_page_index)

        #if frame.location in self.frames:
            #logical_page = self.frames[frame.location].logical_page

        table_name, page_range_index, record_type, logical_page_index = frame.location
        self.disk.write_logical_page(table_name, page_range_index, record_type, logical_page_index, frame.logical_page)
        #self.frames[frame.location].dirty = False #clean
        
        frame.dirty = False
    # Write back all dirty pages (called when closing the db and saving to disk)
    def write_back_all_dirty_frames(self):
        for location, frame in self.frames.items(): 
            if frame.dirty: 
                table_name, page_range_index,record_type, logical_page_index = location
                self.disk.write_logical_pages(table_name,page_range_index,record_type,logical_page_index,frame.logical_page) #uses stored location modifying the logical page back to disk
                frame.dirty = False

    def pin_frame(self, frame):
        """ Prevents logical page from being evicted """
        if frame.pinned:
            return

        if self.num_pinned >= Config.NUM_FRAMES:
            raise RuntimeError("MAXED PINS. CANNOT PIN MORE")
    
        if not frame.pinned:
            frame.pinned = True
            self.num_pinned += 1

    def unpin_frame(self, frame):
        """ allows logical page to be evicted"""
        if frame.pinned:
            frame.pinned = False
            self.num_pinned -= 1

    def unpin_all_frames(self): # for deadlocking 
        for frame in self.frames.values():
            self.unpin_frame(frame)

# References a logical page
class Frame():
    def __init__(self, logical_page, location):
        self.dirty = False
        self.pinned = False
        self.location = location # tuple: (table_name, page_range_index, record_type, logical_page_index)
        self.logical_page = logical_page # Holds the in-memory logical page
