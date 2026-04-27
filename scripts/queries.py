#!/usr/bin/env python3
import kuzu
from pathlib import Path
import os
import sys

from pipeline.config import load_config

config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
db_path = str(load_config(config_path).graph.db_path)

if not os.path.exists(db_path):
    print(f"Error: Database at {db_path} does not exist.")
    print("Run '.venv/bin/invoice-pipeline graph' first to ingest data.")
    exit(1)

print(f"Connecting to Kuzu DB at {db_path}...")
db = kuzu.Database(db_path)
conn = kuzu.Connection(db)

def run_query(desc, query):
    print(f"\n--- {desc} ---")
    print(f"Query: {query}")
    try:
        results = conn.execute(query)
        # Fetching column names
        column_names = results.get_column_names()
        print("\nResults:")
        print(" | ".join(column_names))
        print("-" * 50)
        
        while results.has_next():
            row = results.get_next()
            print(" | ".join([str(item) for item in row]))
    except Exception as e:
        print(f"Error: {e}")

# 1. Show all vendors and their total invoice counts
run_query(
    "Vendors and their invoice counts",
    "MATCH (v:Vendor)-[:ISSUED]->(i:Invoice) RETURN v.name, count(i) as total_invoices"
)

# 2. Show invoices and their line items
run_query(
    "Invoices with their Line Items",
    "MATCH (i:Invoice)-[:CONTAINS]->(li:LineItem) RETURN i.invoice_number, i.invoice_date, li.description, li.amount"
)

# 3. Find the total sum of all invoices
run_query(
    "Total value of all processed invoices",
    "MATCH (i:Invoice) RETURN sum(i.total) as grand_total"
)
