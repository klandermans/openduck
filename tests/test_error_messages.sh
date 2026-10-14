#!/bin/bash
# Test script to demonstrate improved error messages

echo "Testing improved error messages in openduck.py"
echo "================================================"
echo ""

# Create a test directory
TEST_DIR="/tmp/openduck_test_$$"
mkdir -p "$TEST_DIR"

# Create different types of problematic files
echo "Creating test files..."

# 1. Empty CSV file
touch "$TEST_DIR/empty.csv"
echo "✓ Created empty.csv"

# 2. CSV with inconsistent columns
echo "col1,col2,col3" > "$TEST_DIR/bad_columns.csv"
echo "value1,value2" >> "$TEST_DIR/bad_columns.csv"
echo "value1,value2,value3,value4" >> "$TEST_DIR/bad_columns.csv"
echo "✓ Created bad_columns.csv"

# 3. A non-CSV file with .csv extension
echo "This is not a CSV file" > "$TEST_DIR/fake.csv"
echo "✓ Created fake.csv"

# 4. Binary file with .csv extension
dd if=/dev/urandom of="$TEST_DIR/corrupted.csv" bs=1024 count=1 2>/dev/null
echo "✓ Created corrupted.csv"

echo ""
echo "Test files created in: $TEST_DIR"
echo ""
echo "To test the improved error messages:"
echo "1. Open openduck.py"
echo "2. Navigate to: $TEST_DIR"
echo "3. Try opening any of these files"
echo "4. You should see clear, descriptive error messages"
echo ""
echo "Error types you'll see:"
echo "  ❌ Empty or invalid CSV file"
echo "  ❌ CSV format error (inconsistent delimiters)"
echo "  ❌ Cannot read CSV file (corrupted/unsupported format)"
echo ""
echo "To clean up test files, run:"
echo "  rm -rf $TEST_DIR"
