from lstore.config import Config

class Bufferpool():
    def __init__(self):
        self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
    


# References a physical page
class Frame():
    def __init__(self):
        self.state = Config.EMPTY_STATE # Empty, full, or dirty
        self.logical_page = None # Holds the in-memory logical page