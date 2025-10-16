
# L-Store Database System

L-Store is a custom database management system (DBMS) implemented in Python, designed to demonstrate the core principles of modern storage architectures. It supports **record versioning, merge/compaction, and transactional updates** using a columnar layout. The system was built from the ground up to showcase how real-world databases maintain high performance and consistency under concurrent operations.

## Features

* **Record versioning** for multi-version concurrency control (MVCC)
* **Merge and compaction** process to reconcile base and tail records
* **Transaction support** with atomic updates and rollback mechanisms
* **Columnar storage layout** for efficient reads and analytics
* **Indexing** for fast record lookup by primary key
* **Page-based buffer management** to simulate physical storage
* **Custom test harness** for performance and correctness validation

## Technologies

* Python 3  
* NumPy (for columnar storage optimization)  
* Custom-built in-memory data structures (no external DB libraries)  
* Modular architecture designed for extensibility and experimentation  

## System Overview

The system simulates a hybrid storage model inspired by modern analytical databases.  
Each table contains:
- **Base pages:** Immutable records for stable data.
- **Tail pages:** Append-only structures storing updates.
- **Merge process:** Periodically consolidates base and tail data to maintain performance and minimize fragmentation.

Transactions ensure **atomic writes** and **consistency**, while versioning enables **isolation** across concurrent operations.  

## Prerequisites

Before running the system, ensure you have:

* **Python 3.8+**
* **NumPy** installed (`pip install numpy`)

Optional (for testing):
* **pytest** for automated correctness checks

## Installation & Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/L-Store-DBMS.git
   cd L-Store-DBMS
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the test harness:**

   ```bash
   python3 tester.py
   ```

   This runs the built-in test suite to verify record insertion, updates, reads, and merges.

## Usage

You can interact with the system by importing its classes in a Python shell or script.

Example:

```python
from lstore.db import Database
from lstore.table import Table

# Create a new database and table
db = Database()
grades_table = db.create_table('Grades', 5, 0)

# Insert records
grades_table.insert(1, 90, 95, 80, 100, 85)

# Update and read records
grades_table.update(1, 92, None, 85, None, 88)
record = grades_table.select(1, [1, 1, 1, 1, 1, 1])
print(record)
```

## Performance & Testing

Performance and correctness were validated using a **custom test harness** designed to simulate real-world workloads. The test suite measures:

* **Insertion throughput:** verifies efficient page allocation and indexing.  
* **Update latency:** ensures version chains and merges maintain sublinear lookup time.  
* **Merge efficiency:** evaluates compaction performance across various update frequencies.  
* **Transaction integrity:** confirms atomicity and isolation under concurrent updates.  

All test cases were designed to validate both **logical correctness** and **performance stability** over time. The system achieved high consistency under mixed read/write loads while maintaining correct version ordering and minimal merge overhead.

## Value

This project provides hands-on insight into how database engines handle **data versioning, merging, and transactional consistency**—the same core concepts underlying enterprise systems like PostgreSQL, SQL Server, and Snowflake.  
It serves as a strong educational foundation for anyone studying **database internals**, **storage design**, or **system-level performance optimization**.

## Project Structure

```
L-Store-DBMS/
│
├── lstore/
│   ├── db.py           # Database entry point
│   ├── table.py        # Table and page structure
│   ├── page.py         # Base/tail page management
│   ├── record.py       # Record versioning logic
│   └── index.py        # Simple indexing structure
│
├── tester.py           # Built-in correctness and performance tests
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

## Notes

* The system is **educational**, not production-grade.  
* Merge operations may take time depending on table size and update frequency.  
* Data is stored in-memory by default but can be extended to use persistent storage.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
