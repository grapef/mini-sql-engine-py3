#!/bin/sh
python3 -m pip install --quiet sqlparse prettytable
python3 mini_sql.py "$@"
