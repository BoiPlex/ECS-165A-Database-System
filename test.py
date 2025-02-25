from lstore.physical_page import PhysicalPage
from lstore.logical_page import LogicalPage
from lstore.page_range import PageRange
from lstore.table import Table, Record
from lstore.db import Database
from lstore.index import Index
from lstore.query import Query
from lstore.config import Config

# ----------------------------------------------------------------


db = Database()

table = db.create_table("Bruh", 3, 0)
query = Query(table)

# Insert
query.insert(1, 4, 18) # rid=1

# query.update(1, None, 12, 13) # rid=2
# query.update(1, None, 14, 15) # rid=3
# query.update(1, None, 16, 17) # rid=4

record_list = query.select_version(1, 0, [1, None, 1], 0)
for record in record_list:
    print(record.columns)

db.close() 
print("close the database and write it to disk")

db_reopened = Database() 
db_reopened.open() # what would the p

# # Select
# record_list = query.select(18, 2, [1, 1, 1])
# for record in record_list:
#     print(record.columns)

# # Sum
# sum = query.sum(1, 2, 2)
# print(sum)

# ----------------------------------------------------------------
# TEST: Table

# table = Table("Bruh", 3, 0)

# # New base records
# print("Create:", table.create_record([1, 2, 3]))
# print("Create:", table.create_record([4, 5, 6]))

# # New tail record
# print("Update:", table.update_record(2, 0, 69))

# # Mark record to delete
# print("Delete:", table.delete_record(1))

# # Read base records
# print("Read:", table.read_record(1))
# print("Read:", table.read_record(2))

# ----------------------------------------------------------------
# TEST: PageRange

# PR = PageRange(3)

# # Fill up the first base page
# for i in range(Config.MAX_RECORDS_PER_LOGICAL_PAGE):
#     PR.create_record(Config.BASE_RECORD, [1, 2, 3])

# PR.create_record(Config.BASE_RECORD, [69, 420, 69])

# PR.create_record(Config.TAIL_RECORD, [1, 2, 1])
# PR.update_tail_record_value(0, 0, 2, 8)
# PR.mark_to_delete_record(Config.TAIL_RECORD, 0, 0)

# # Print first 3 records of first 2 base pages
# print("BASE PAGES:")
# for i in range(3):
#     print(PR.read_record(Config.BASE_RECORD, 0, i))
# print()
# for i in range(3):
#     print(PR.read_record(Config.BASE_RECORD, 1, i))
# print()

# # Print first 3 records of first tail page
# print("TAIL PAGE:")
# for i in range(3):
#     print(PR.read_record(Config.TAIL_RECORD, 0, i))

# ----------------------------------------------------------------
# TEST: LogicalPage (base page for testing)

# TP = LogicalPage(3)

# TP.create_record([1, 2, 3])
# TP.create_record([7, 5, 1])
# TP.create_record([239, 23, 964])

# TP.update_record_value(0, 1, 69)
# TP.mark_to_delete_record(0)

# # First 5 offsets/records/rows
# for i in range(5):
#     print(TP.read_record(i))

# ----------------------------------------------------------------
# TEST: PhysicalPage

# col = PhysicalPage()

# col.create(20) # becomes 0
# col.create(35) # becomes large number
# col.create(3)
# col.create(34)

# col.update_value(1, 2)

# for i in range(5):
#     print(col.read(i))
# ----------------------------------------------------------------
#TEST: Index


