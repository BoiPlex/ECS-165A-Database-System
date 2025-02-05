from physical_page import PhysicalPage
from logical_page import LogicalPage


# ----------------------------------------------------------------
# TEST: Table



# ----------------------------------------------------------------
# TEST: PageRange



# ----------------------------------------------------------------
# TEST: LogicalPage (base page for testing)

# BP = LogicalPage(3)

# ----------------------------------------------------------------
# TEST: PhysicalPage

col = PhysicalPage()

col.write(0, 69)
col.write(1, 420)

num = col.read(1)
print(num)

# for i, num in enumerate(col.data):
#     print(f"{i}: {num}")

