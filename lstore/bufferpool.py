from lstore.config import Config
from collections import OrderedDict

class Bufferpool():
    def __init__(self):
        self.page_table = OrderedDict()
        self.frames = {}
        # self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
    
    def request_page(self, rid):
        """ Returns a page if its in memory, will load from disk if not """
        if len(self.frames) >= Config.NUM_FRAMES: # Checks if bufferpool is full
            self.evict_page()
            return self.load_page_from_disk(rid)

        if rid in self.page_table:
            self.page_table.move_to_end(rid) # Maitains LRU
            return self.frames[rid]
        
        return self.load_page_from_disk(rid)
    
    def evict_page(self):
        """ Removes LRU page from memory """
        evict_rid, _=self.page_table.popitem(last=False)
        del self.frames[evict_rid]
        return

    def load_page_from_disk(self, rid):
        """ Loads page from disk into memory. """
        #physical_page = self.read_from_disk(rid)  # Implementation Needed !
        frame = Frame(physical_page)
        self.frames[rid] = frame
        self.page_table[rid] = frame  # Add to LRU tracking
        return frame

    def write_back_page(self, rid):
        """ writes dirty page back to disk. """
        physical_page = self.frames[rid].physical_page 
        self.write_to_disk(rid, physical_page)  # Implementation Needed !
        self.frames[rid].dirty = False
      
    

    def pin_page(self, rid):
        if rid in self.frames:
            self.frames[rid].pinned = True

    def unpin_page(self, rid):
        if rid in self.frames:
            self.frames[rid].pinned = False

# References a logical page
class Frame():
    def __init__(self, physical_page):
        self.state = "Empty" # Full, dirty, or empty
        self.physical_page = physical_page # Holds the in-memory physical page
        self.dirty = False
        self.pinned = False
        # self.state = Config.EMPTY_STATE # Empty, full, or dirty
        # self.logical_page = None # Holds the in-memory logical page
    