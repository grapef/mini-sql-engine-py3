# About
This repository is a Python 3 update of the original Mini SQL Engine from [Priyansh2/SQL-Engine](https://github.com/Priyansh2/SQL-Engine/tree/master). The engine parses and executes a subset of DML queries via the command line on CSV-backed data. We upgraded it for Python 3 compatibility and trimmed the dataset to a minimal example with an accompanying sample runner.

Following cases of query processing are handled as of now.
- Blank column entries while entering data
- Single and double quoted values in CSV files
- Multiple AND/OR queries, including nested ones
- Operators handled in column comparison: `=, !=, >, <, <=, >=`
- Aggregate functions: `distinct, sum, min, max, avg.`
- Pretty Table output
- Summary of successful query
- Table aliasing

## Requirements
- Python 3.8+
- SQL Parse (```pip install sqlparse```)
- Pretty Table (```pip install prettytable```)

## Files
- ```mini_sql.py```: Main engine
- ```metadata.txt```: Table/column metadata for the bundled datasets (`employees.csv`)
- ```samplerun.py```: Simple sample runner demonstrating SELECT and WHERE queries on `employees.csv`
- ```mini_sql.sh```: Helper to install deps and run with `python3`
- ```instructions.md```: Brief usage notes
  
## NOTE
1. Column name should not start with a number but can contain a table name.
2. Query case is insensitive.
3. When selecting from multiple tables, provide an equijoin condition; otherwise, a cross join is performed.
4. For multiple AND/OR conditions, enclose them in parentheses.

## Attribution
Based on the original project: [Priyansh2/SQL-Engine](https://github.com/Priyansh2/SQL-Engine/tree/master). This fork updates compatibility to Python 3 and includes a slimmed-down dataset plus sample runner for quick testing.
