
"""

Datbase
- Table[] size: infinite

Table
- page_range[] size: infinite

page_range
- base_page[16]
- tail_page[] size: infinite

logical_page (base_page or tail_page)
- physical_page[user_cols + meta_cols]

physical_page (4096 bytes)
- 512 records of 8 bytes


"""