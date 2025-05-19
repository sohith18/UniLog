# UniLog: Data Synchronization Across Heterogeneous Database Systems

A comprehensive implementation of data synchronization and integration across heterogeneous database systems (MongoDB, Hive, and PostgreSQL) with an operation log-based approach for eventual consistency.

## Project Overview

UniLog demonstrates a robust solution for data synchronization across distributed heterogeneous database systems. The project implements core operations (GET, SET, MERGE) to maintain data consistency across MongoDB, Hive, and PostgreSQL databases, using operation logs (oplogs) to track and reconcile changes.

### Features

- **Independent Database Operations**: Each system supports autonomous read and write operations
- **Operation Logging**: Comprehensive logging of all operations with timestamps for synchronization
- **Timestamp-based Versioning**: Uses logical timestamps to establish operation order
- **Last-Writer-Wins Conflict Resolution**: Resolves conflicts by prioritizing the most recent operation
- **Mathematically Sound Merge Properties**: Ensures commutativity, associativity, and idempotency
- **Eventual Consistency**: Guarantees that all systems will eventually reach identical states

## Architecture

The project is structured into three primary modules, each handling a specific database system:

1. **Hive Implementation**: Manages Apache Hive database interactions and synchronization
2. **MongoDB Implementation**: Handles MongoDB operations and log management
3. **PostgreSQL Implementation**: Supports PostgreSQL database operations and synchronization

Each module provides a consistent interface with the following key components:

- **Connection Management**: Handles database connectivity
- **Timestamp Cache**: Tracks the most recent updates for each key
- **Operation Log Management**: Records and retrieves operation history
- **Table Management**: Handles schema and data operations
- **Merge Logic**: Synchronizes state using oplogs from other systems

## Core Operations

### GET

Retrieves data for a specified composite key (student_id, course_id) and logs the operation.

```
[Timestamp], [System].GET(student_id, course_id)
```

### SET

Updates a grade value for a specified composite key and logs the operation with a timestamp.

```
[Timestamp], [System].SET((student_id, course_id), grade)
```

### MERGE

Synchronizes the current system's state with another system based on its operation log.

```
[System1].MERGE([System2])
```

## Operation Log Format

The project uses a standardized operation log format across all systems:

```json
{
  "timestamp": 1,
  "operation": "SET",
  "table": "student_course_grades",
  "keys": {
    "student_id": "SID101",
    "course_id": "CSE026"
  },
  "item": {
    "grade": "A"
  }
}
```

## Mathematical Properties of Merge Operations

The implementation ensures the following properties:

- **Commutativity**: A.MERGE(B) followed by B.MERGE(A) = B.MERGE(A) followed by A.MERGE(B)
- **Associativity**: (A.MERGE(B)).MERGE(C) = A.MERGE((B.MERGE(C)))
- **Idempotency**: A.MERGE(B) followed by A.MERGE(B) = A.MERGE(B)
- **Eventual Consistency**: All systems will eventually converge to identical states

## Repository Structure

```
UniLog/
├── hive/            # Hive implementation: connection, table, log, sync logic
├── mongo/           # MongoDB implementation with MongoService class
├── postgresql/      # PostgreSQL implementation and log manager
├── dataset/         # (Optional) Sample data – not required for framework use
├── testcase.in      # Sample sequence of SET, GET, MERGE commands
├── main.py          # Parses test cases and triggers appropriate backend ops
└── README.md        # You're reading it!
```

## Setup and Installation

### Prerequisites

- Python 3.7+
- MongoDB
- Apache Hive
- PostgreSQL
- Required Python packages (pymongo, PyHive, psycopg2, pandas)

### Installation and Running

1. Clone the repository:
   ```
   git clone https://github.com/sohith18/UniLog.git
   cd UniLog
   ```

2. Run the main script to execute the test cases:
   ```
   python main.py
   ```

The main.py file will parse the testcase.in file and execute the sequence of SET, GET, and MERGE operations on the appropriate database systems.

## Test Cases

Sample test cases are provided to demonstrate system behavior:

### Test Case 1: Basic GET and SET Operations
```
1, HIVE.SET((SID103, CSE016), A)
2, HIVE.GET(SID103, CSE016)
```

### Test Case 2: Merge with Another System
```
1, HIVE.SET((SID103, CSE016), A)
3, SQL.SET((SID103, CSE016), B)
HIVE.MERGE(SQL)
```

### Test Case 3: Concurrent Updates with Different Timestamps
```
1, HIVE.SET((SID101, CSE026), C)
2, MONGO.SET((SID101, CSE026), B)
3, SQL.SET((SID101, CSE026), A)
HIVE.MERGE(MONGO)
HIVE.MERGE(SQL)
```

### Test Case 4: Circular Merges
```
1, HIVE.SET((SID103, CSE016), A)
3, SQL.SET((SID103, CSE016), B)
5, MONGO.SET((SID103, CSE016), C)
HIVE.MERGE(SQL)
SQL.MERGE(MONGO)
MONGO.MERGE(HIVE)
```

## Contributions

- **Sai Venkata Sohith Gutta**: Implemented the Hive-based system, designed the timecache mechanism, and made key architectural decisions (36%)
  
- **Aaryan Ajith Dev**: Implemented the MongoDB service class and led the design of a generalized operation log format for seamless merges (32%)
  
- **Shreyas S**: Implemented data integration and synchronization for PostgreSQL (32%)

