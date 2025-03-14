from lstore.config import Config
from lstore.table import Table, Record
from lstore.index import Index
import threading
import time
import random

class TransactionWorker:
    transaction_count = 0
    transaction_count_lock = threading.Lock()

    next_transaction_worker_id = 1

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.transaction_worker_id = TransactionWorker.next_transaction_worker_id
        TransactionWorker.next_transaction_worker_id += 1
    
        self.stats = []
        self.transactions = []
        self.result = 0
        self.thread = None
        

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            while True:
                # each transaction returns True if committed, False if aborted, or Config.UNABLE_TO_ACQUIRE_LOCK if unable to acquire lock
                result = transaction.run()
                if result == Config.UNABLE_TO_ACQUIRE_LOCK:
                    time.sleep(random.uniform(0.1, 0.5)) # Wait for a randomized delay
                    # Retry the transaction (next iteration)
                elif result:
                    self.stats.append(True)
                    with TransactionWorker.transaction_count_lock:
                        TransactionWorker.transaction_count += 1
                        # print(self.transaction_worker_id, TransactionWorker.transaction_count)
                    break
                else:
                    # print("Transaction discarded due to an error")
                    self.stats.append(False)
                    # print("FAILED TRANSACTION")
                    break
                    
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

