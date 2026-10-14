#!/usr/bin/env python3
"""
Test script to demonstrate all error message types in openduck.py
"""

import sys
sys.path.insert(0, '/Volumes/dev/openduck')
from openduck import DuckCLI

app = DuckCLI()

print("=" * 70)
print("IMPROVED ERROR MESSAGES IN OPENDUCK.PY")
print("=" * 70)
print()

# Test different error scenarios
test_cases = [
    ("CSV - File not found", 
     Exception("Could not open CSV file"), 
     "/path/to/missing.csv"),
    
    ("CSV - Permission denied", 
     Exception("Permission denied reading CSV file"), 
     "/path/to/protected.csv"),
    
    ("CSV - Encoding error", 
     Exception("Invalid encoding in CSV file"), 
     "/path/to/bad_encoding.csv"),
    
    ("CSV - Delimiter issue", 
     Exception("Inconsistent delimiter in CSV"), 
     "/path/to/bad_delimiter.csv"),
    
    ("CSV - Empty file", 
     Exception("Empty file or no columns found"), 
     "/path/to/empty.csv"),
    
    ("CSV - No columns", 
     Exception("No columns found in CSV"), 
     "/path/to/no_columns.csv"),
    
    ("CSV - Generic error", 
     Exception("Invalid CSV format"), 
     "/path/to/invalid.csv"),
    
    ("Excel - File not found", 
     Exception("No such file: workbook.xlsx"), 
     "/path/to/workbook.xlsx"),
    
    ("Excel - Generic error", 
     Exception("Cannot read Excel file"), 
     "/path/to/corrupted.xlsx"),
    
    ("Parquet - Generic error", 
     Exception("Invalid parquet file"), 
     "/path/to/bad.parquet"),
    
    ("JSON - Generic error", 
     Exception("Invalid JSON syntax"), 
     "/path/to/bad.json"),
    
    ("Generic - File not found", 
     Exception("No such file or directory"), 
     "/path/to/missing.csv"),
    
    ("Generic - Permission denied", 
     Exception("Permission denied"), 
     "/path/to/protected.csv"),
    
    ("Generic - Disk I/O error", 
     Exception("Disk I/O error occurred"), 
     "/path/to/disk_error.csv"),
    
    ("Generic - Out of memory", 
     Exception("Out of memory"), 
     "/path/to/huge.csv"),
    
    ("Generic - Unknown error", 
     Exception("Something went wrong"), 
     "/path/to/unknown.csv"),
]

for i, (test_name, exception, file_path) in enumerate(test_cases, 1):
    print(f"{i}. {test_name}")
    print("-" * 70)
    result = app._classify_file_error(exception, file_path)
    print(result)
    print()

print("=" * 70)
print("All error message tests completed!")
print("=" * 70)
