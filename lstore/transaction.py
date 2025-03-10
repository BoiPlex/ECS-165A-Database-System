from lstore.config import Config
from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

class Transaction:
    next_transaction_id = 0

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.transaction_id = Transaction.next_transaction_id
        Transaction.next_transaction_id += 1

        self.queries = []
        self.rollback_queries = [] # stores original values for rollback
        
        self.lock_manager = None


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        if not self.lock_manager:
            self.lock_manager = table.lock_manager

        self.queries.append((query, args))
        # use grades_table for aborting
        # TODO
        if query is Query.delete:
            primary_key, = args

            rid = self.table.index.key_to_rid(self.table.key, primary_key)

            while True:
                if self.lock_manager.lock_record(rid, self.transaction_id, Config.LOCK_TYPE_SHARED):
                    break
            
            columns = self.table.read_record(rid).columns
            location = self.table.page_directory[rid]
            
            self.lock_manager.unlock_record(rid, self.transaction_id)

            args = (rid, columns, location)

            self.rollback_queries.append((Query.undo_delete, ))
            
        elif query is Query.insert:
            self.rollback_queries.append((Query.delete, ))
        elif query is Query.update:
            self.rollback_queries.append((Query.update, ))


        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args, self.transaction_id)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        # TODO: do roll-back and any other necessary operations

        
        self.lock_manager.unlock_all_records(self.transaction_id)
        return False

    
    def commit(self):
        self.rollback_quires.clear()

        self.lock_manager.unlock_all_records(self.transaction_id)
        return True
