from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed


normal_p1 = True
extended_p1 = True

score = 0
def normal_tester():
    print("Checking exam M3 normal tester")
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    if normal_p1:
        db = Database()
        db.open('./MLS3')

        # creating table
        grades_table = db.create_table('MS3', 5, 0)

        # create a query class for the table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_transactions = 100
        num_threads = 8

        keys = []
        records = {}
        seed(3562901)

        # array of insert transactions
        insert_transactions = []

        for i in range(number_of_transactions):
            insert_transactions.append(Transaction())

        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
            t = insert_transactions[i % number_of_transactions]
            t.add_query(query.insert, grades_table, *records[key])

        transaction_workers = []
        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())
            
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()


        # Check inserted records using select query in the main thread outside workers
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                print('select error on', key, ':', record, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        print("Select finished")

        # dictionary for records to test the database: test directory
        number_of_operations_per_record = 1

        transaction_workers = []
        transactions = []

        for i in range(number_of_transactions):
            transactions.append(Transaction())

        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())



        updated_records = {}
        # x update on every column
        for j in range(number_of_operations_per_record):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                updated_records[key] = records[key].copy()
                for i in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    updated_records[key][i] = value
                transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
                transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)
        print("Update finished")


        # add trasactions to transaction workers  
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(transactions[i])


        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()

        
        v0_score = len(keys)
        for key in keys:
            correct = updated_records[key]
            query = Query(grades_table)
            
            result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
            print(result)
            if correct != result:
                raise Exception('select error on primary key', key, ':', result, ', correct:', correct)
                v0_score -= 1
        print('Version 0 Score:', v0_score, '/', len(keys))
        score = score + 10

        valid_sums = 0
        number_of_aggregates = 100

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum == result:
                valid_sums += 1
        print("Aggregate version 0 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)
        score = score + 10
        db.close()


def extended_tester():
    print("Checking exam M3 extended tester")
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    if extended_p1:
        db = Database()
        db.open('./M3')

        # creating grades table
        grades_table = db.create_table('TableM3', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_transactions = 100
        num_threads = 8

        keys = []
        records = {}
        seed(3562901)

        # array of insert transactions
        insert_transactions = []

        for i in range(number_of_transactions):
            insert_transactions.append(Transaction())

        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
            t = insert_transactions[i % number_of_transactions]
            t.add_query(query.insert, grades_table, *records[key])

        transaction_workers = []
        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())
            
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()


        # Check inserted records using select query in the main thread outside workers
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                print('select error on', key, ':', record, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        print("Select finished")

        number_of_updates = 4
        number_of_aggregates = 5

        all_updates = []    
        for i in range(number_of_updates):
            all_updates.append({})
            transactionWorker = TransactionWorker()
            for key in keys:
                transaction = Transaction()
                updated_columns = [None, None, None, None, None]
                # copy record to check
                all_updates[i][key] = records[key].copy()
                for j in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[j] = value
                    # update our test directory
                    all_updates[i][key][j] = value
                transaction.add_query(query.update, grades_table, key, *updated_columns)
                transactionWorker.add_transaction(transaction)
            transactionWorker.run()
            transactionWorker.join()
        print("Update finished")


        try:
            # Check records that were persisted in part 1
            for version in range(1):
                expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
                for key in keys:
                    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                    error = False
                    for k, column in enumerate(record.columns):
                        if column != expected_update[key][k]:
                            error = True
                    if error:
                        raise Exception('select error on', key, ':', record.columns, ', correct:', expected_update[key])
                        break
                print("Select version ", version, "finished")
            score = score + 15
        except Exception as e:
            print("Something went wrong during select")
            print(e)

        try:
            for version in range(1):
                expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
                for j in range(0, number_of_aggregates):
                    r = sorted(sample(range(0, len(keys)), 2))
                    column_sum = sum(map(lambda x: expected_update[x][0] if x in expected_update else 0, keys[r[0]: r[1] + 1]))
                    result = query.sum(keys[r[0]], keys[r[1]], 0)
                    if column_sum != result:
                        raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
                print("Aggregate version ", version, "finished")
            score = score + 15
        except Exception as e:
            print("Something went wrong during sum")
            print(e)
        db.close()

from timeit import default_timer as timer
from decimal import Decimal

import os
import glob
import traceback
import shutil
import sys   
    
def run_test():
    global normal_p1, normal_p2, extended_p1, extended_p2
    if len(sys.argv) > 1:
        normal_p1 = "np1" in sys.argv
        normal_p2 = "np2" in sys.argv
        extended_p1 = "ep1" in sys.argv
        extended_p2 = "ep2" in sys.argv
    start = timer()
    try: 
        if normal_p1:
            # Remove all files before executing extended tester
            files = glob.glob(os.path.join(os.getcwd(), "MLS3*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                else: shutil.rmtree(f)
        normal_tester()
        # Remove all files before executing extended tester
        if extended_p1:
            files = glob.glob(os.path.join(os.getcwd(), "M3*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                else: shutil.rmtree(f)
        extended_tester()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()
    end = timer()
    print("\n------------------------------------")
    print("Time taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
    print("Total score: ", score)
    print("Normalized total Score for M3:", str(score)+"/50")
    print("--------------------------------------\n")

run_test();        

print("Done M3 Part 1")