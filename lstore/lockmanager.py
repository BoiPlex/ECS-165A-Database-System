import threading

class LockManager:
    def __init__(self):
        self.locks = {} # 
        self.locks_lock = threading.lock()

    """
    The get_lock() function requires a record ID, transaction ID, and lock type.
    - The transaction ID tracks which transactions possess which record locks, effectively preventing other transactions from modifying
      a record without permission. Without it, any transaction could release any lock. This also allows for rollback on transaction failures or aborts.
      Also allows deadlock detection (when transactions are waiting forever for locks).
    - RID obviously tracks which record is being transacted upon.
    - Lock type determins the actions that can be imposed upon a record.
      -> Type "S" is a Shared Lock or Read Lock, which multiple transactions can hold at once, as it only allows them to read a record.
          However, while multiple transactions can hold an "S" lock on a record, no other lock types can be held on that record.
      -> Type "X" is an Exclusive Lock or Write Lock, which only one transactions can hold at a time, as a record is being modified.
      -> Type "U" is an Update Lock, preventing deadlocks. 
      -> Other lock types (which we probably won't use) include 
            --> "IS" (Intention Shared): Transaction intends to acquire "S" locks on specific rows
            --> "IX" (Intention Exclusive): Transaction intends to acquire "X" locks on specific rows
            --> "SIX" (Shared-Intention Exclusive): Allows reading table while preparing for updates

    Specific use of get_lock():
      1. Adding a lock if it is not in self.locks
      2. Aborting if exclusive lock is held already (No-Wait policy)
      3.  Aborting if we can't upgrade a shared lock to exclusive ("S" to "X") (No-Wait policy)
      4. Returning true if lock is acquired
    """
    def get_lock(self, rid, transaction_id, lock_type): # 
        pass

    """
    The unlock_record() function unlocks or relases a record that is held by a transaction if it is in self.locks
    """
    def unlock_record(self, rid, transaction_id):
        pass
        
    """
    The unlock_all_records() function released all locks held by a specific transaction, as opposed to a lock on a specific record as is above
    """
    def unlock_all_records(self, transaction_id):
        pass
    