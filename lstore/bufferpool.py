from lstore.config import Config

class Bufferpool():
    def __init__(self):
        self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
        pass


# References a physical page
class Frame():
    def __init__(self):
        self.state # Full, dirty, or empty

        # Physical page location
        self.page_range_index
        self.record_type
        self.logical_page_index
        self.physical_page_index
        pass