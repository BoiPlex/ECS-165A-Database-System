from lstore.config import Config

class Bufferpool():
    def __init__(self):
        # self.frames = [Frame() for i in range(Config.NUM_FRAMES)]
        pass


# References a physical page
class Frame():
    def __init__(self):
        self.state # Full, dirty, or empty

        self.physical_page # Holds the in-memory physical page
        pass