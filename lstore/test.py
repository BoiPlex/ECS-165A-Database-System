from physical_page import PhysicalPage
from logical_page import LogicalPage
from page_range import PageRange
from table import Table
from config import Config

# ----------------------------------------------------------------
# TEST: Table

table = Table("Bruh", 3, 0)

print(table.create_record([1, 2, 3]))

# Continue Table tests here...

# print(table.read_record(1))

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

# TP.update_record(0, 1, 69)
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

# col.write(1, 2)

# for i in range(5):
#     print(col.read(i))

# for i, num in enumerate(col.data):
#     print(f"{i}: {num}")

