from lstore.config import Config
from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.lock import Lock

class Transaction:
    next_transaction_id = 0

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.transaction_id = Transaction.next_transaction_id
        Transaction.next_transaction_id += 1

        self.queries = []
        self.locks = [] # List of tuples, each is (Lock, lock_type) the transaction must acquire and release

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

        # Pass necessary locks for query
        self.locks.extend(self.get_query_locks(query, args, table))

        # use table for aborting
        # TODO
        if query is Query.delete:
            primary_key, = args # unpack args to 1 variable

            # --------------------
            # Get necessary info for rollback_delete query
            rid = table.index.key_to_rid(table.key, primary_key)
            record_lock: Lock = self.lock_manager.get_lock(rid)
            while True:
                if record_lock.acquire_lock(self.transaction_id, Config.LOCK_TYPE_SHARED):
                    break
            
            columns = table.read_record(rid).columns
            location = table.page_directory[rid]
            page_range_index, record_type, base_page_index, base_offset_index = location
            indirection_rid = table.page_ranges[page_range_index].read_record_column(record_type, base_page_index, base_offset_index, Config.INDIRECTION_COLUMN)
            
            record_lock.release_lock(self.transaction_id)
            # --------------------

            rollback_args = (rid, columns, location, indirection_rid)
            rollback_lock_tuple = (record_lock, Config.LOCK_TYPE_EXCLUSIVE)
            self.rollback_queries.append((Query.rollback_delete, rollback_args, rollback_lock_tuple))
            
        elif query is Query.insert:
            columns = args # args tuple is exactly equal to columns
            primary_key = columns[table.key]

            # Insert's rollback just does a delete
            # sussy balls i love fortnite <3 IMPORTANT!!!
            rollback_args = (primary_key)
            rollback_lock_tuple = self.get_query_locks(Query.delete, rollback_args, table)[0]
            self.rollback_queries.append((Query.delete, rollback_args, rollback_lock_tuple))
        
        elif query is Query.update:
            primary_key, *columns = args

            # Update's rollback sets base record's indirection from new latest back to original latest
            
            # --------------------
            # Get necessary info for rollback_update query
            rid = table.index.key_to_rid(table.key, primary_key)
            record_lock: Lock = self.lock_manager.get_lock(rid)
            while True:
                if record_lock.acquire_lock(self.transaction_id, Config.LOCK_TYPE_SHARED):
                    break
            
            # GET STUFF
            prev_indirection_rid = table.get_next_lineage_rid(Config.BASE_RECORD, rid)

            record_lock.release_lock(self.transaction_id)
            # --------------------

            rollback_args = (rid, prev_indirection_rid)
            rollback_lock_tuple = (record_lock, Config.LOCK_TYPE_EXCLUSIVE)
            self.rollback_queries.append((Query.rollback_update, rollback_args, rollback_lock_tuple))

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # acquire all locks, if unable then return Config.UNABLE_TO_ACQUIRE_LOCK that tells TransactionWorker to retry the transaction
        self.acquire_all_locks()

        for query_index, (query, args) in enumerate(self.queries):
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                self.abort(query_index)
                return False
        return self.commit()

    
    def abort(self, last_query_index):
        # TODO:

        # release all original locks
        self.release_all_locks()
        
        # do rollback
        for (rollback_query, rollback_args, rollback_lock_tuple) in self.rollback_queries:
            lock, lock_type = rollback_lock_tuple
            while True:
                if lock.acquire_lock(self.transaction_id, lock_type):
                    break
            rollback_query(*rollback_args)
            lock.release_lock(self.transaction_id)
                    
        return True

    
    def commit(self): 
        self.rollback_quires.clear()

        # release all locks
        self.release_all_locks()

        return True
    

    # Alternative strat: Return true/false rather than busy waiting
    def acquire_all_locks(self):
        for lock, lock_type in self.locks:
            while True:
                if lock.acquire_lock(self.transaction_id, lock_type):
                    break

    def release_all_locks(self):
        for lock, lock_type in self.locks:
            while True:
                if lock.release_lock(self.transaction_id):
                    break

    
    """
    ACQUIRE (actually get) LOCKS WOOOHOOOO
    i need adderrallaeof;a    my roommate sells jk :( totally not illegal jajajaja
    its a medication given to people with adhd - consider it a "LOCKED-IN" drug
    ^ Don't delete this, this is important documentation 100%
    Return list of tuple(s), each is (Cock, cock_type, lube)
    """
    def get_query_locks(self, query, args, table):
        if query is Query.select or query is Query.select_version:
            search_key, search_key_index, projected_columns_index = args[:3] # haha :3 we are so helpful
            rids = table.index.locate(search_key_index, search_key)
            for rid in rids:
                record_lock: Lock = self.lock_manager.get_lock(rid)
                self.locks.append((record_lock, Config.LOCK_TYPE_SHARED))
        
        elif query is Query.sum or query is Query.sum_version:
            start_range, end_range, aggregate_column_index = args[:3]
            rids = table.index.locate(search_key_index, search_key)
            for rid in rids:
                record_lock: Lock = self.lock_manager.get_lock(rid)
                self.locks.append((record_lock, Config.LOCK_TYPE_SHARED))
            
        elif query is Query.delete:
            primary_key = args[0]
            rid = table.index.key_to_rid(table.key, primary_key)
            record_lock: Lock = self.lock_manager.get_lock(rid)
            self.locks.append((record_lock, Config.LOCK_TYPE_EXCLUSIVE))
                
        elif query is Query.insert:
            columns = args
            primary_key = columns[self.table.key]
            rid = table.index.key_to_rid(table.key, primary_key)
            record_lock: Lock = self.lock_manager.get_lock(rid)
            self.locks.append((record_lock, Config.LOCK_TYPE_EXCLUSIVE))
            
        elif query is Query.update:
            primary_key, *columns = args
            rid = table.index.key_to_rid(table.key, primary_key)
            record_lock: Lock = self.lock_manager.get_lock(rid)
            self.locks.append((record_lock, Config.LOCK_TYPE_EXCLUSIVE))