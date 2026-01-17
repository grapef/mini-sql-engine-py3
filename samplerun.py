#!/usr/bin/env python3
"""Quick sample runner for the Mini SQL Engine using the bundled employees data."""

import sqlparse

import mini_sql


def run_query(raw_query: str) -> None:
    """Load metadata, run a single query string, and print the result table."""
    mini_sql.generate_schema_and_load_data("metadata.txt")
    formatted = sqlparse.format(raw_query, reindent=True, keyword_case="upper")
    distinct_flag, new_schema, cols, final_dataset, agg_map = mini_sql.query_handling(
        formatted
    )
    mini_sql.display_result(distinct_flag, new_schema, agg_map, cols, final_dataset)


if __name__ == "__main__":
    sample_queries = [
        "select * from employees",
        "select id, salary from employees where salary > 70000",
    ]

    for q in sample_queries:
        print(f"\nQuery: {q}")
        run_query(q)
