from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

insert_time_0 = process_time()
insertSuccess = 0
for i in range(0, 10000):
    insertResult = query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
    if insertResult:
        insertSuccess += 1
insert_time_1 = process_time()
insertSuccessRate = insertSuccess/10000
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
print("\t\t\t\t\t\tInsert Success Rate = ", insertSuccessRate)

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
updateSuccess = 0
for i in range(0, 10000):
    updateResult = query.update(choice(keys), *(choice(update_cols)))
    if updateResult:
        updateSuccess +=1
updateSuccessRate = updateSuccess/10000
update_time_1 = process_time()
updateTime = update_time_1-update_time_0
print("Updating 10k records took:  \t\t\t", updateTime)
print("\t\t\t\t\t\tUpdate Success Rate = ", updateSuccessRate)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    result = query.select(choice(keys), 0, [1, 1, 1, 1, 1])
    if result is False:
        print("FAIL SELECT")
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
    if result is False:
        print("FAIL SUM")
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    result = query.delete(906659671 + i)
    if result is False:
        print("FAIL DELETE")
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)