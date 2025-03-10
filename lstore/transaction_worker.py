from lstore.table import Table, Record
from lstore.index import Index
import threading
import time
import random

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
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
                # each transaction returns True if committed or False if aborted
                success = transaction.run()
                if success:
                    self.stats.append(True)
                    break
                else:
                    print("Transaction aborted. Retrying...")
                    time.sleep(random.uniform(0.1, 0.5)) # Wait for a randomized delay before retrying
                    
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

