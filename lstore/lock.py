from lstore.config import Config
import threading

class Lock:
    def __init__(self):
        self.reading_count = 0
        self.writing_count = 0
        self.owners = set()
        self.lock = threading.Lock() # Prevent multiple threads from accessing this lock object simultaneously

    # General function that acquires either a shared or exclusive lock
    def acquire_lock(self, transaction_id, lock_type):
        if lock_type == Config.LOCK_TYPE_SHARED:
            return self.acquire_shared_lock(transaction_id)
        else:
            return self.acquire_exclusive_lock(transaction_id)
    
    # For reading, return true/false if able/unable to acquire lock
    def acquire_shared_lock(self, transaction_id):
        with self.lock:
            if self.writing_count == 0:
                self.reading_count += 1
                self.owners.add(transaction_id)
                return True
            return False

    # For writing, return true/false if able/unable to acquire lock
    def acquire_exclusive_lock(self, transaction_id):
        with self.lock:
            if self.writing_count == 0 and self.reading_count == 0:
                self.writing_count = 1
                self.owners.add(transaction_id)
                return True
            return False
    
    # Release either a shared or exclusive lock
    def release_lock(self, transaction_id):
        with self.lock:
            if transaction_id in self.owners:
                self.owners.remove(transaction_id)
                if self.reading_count > 0:
                    self.reading_count -= 1
                elif self.writing_count > 0:
                    self.writing_count = 0
                return True
        return False
            