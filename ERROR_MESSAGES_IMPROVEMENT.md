# Improved Error Messages in openduck.py

## Summary
Enhanced error handling in openduck.py to provide clear, distinct, and helpful error messages when files cannot be loaded.

## Changes Made

### 1. New Error Classification Method: `_classify_file_error()`
This method analyzes errors and provides user-friendly messages based on:

#### CSV Files
- **File not found**: Shows the path and suggests checking if file exists
- **Permission denied**: Clear permission error message
- **Encoding errors**: Suggests saving with UTF-8 encoding
- **Format errors**: Detects inconsistent delimiters/columns
- **Empty files**: Indicates when CSV is empty or has no valid columns
- **General errors**: Suggests corruption or unsupported format

#### Excel Files
- **File not found**: Clear path information
- **Permission denied**: Permission error message
- **General errors**: Mentions corruption, password protection, or unsupported format

#### Other File Types
- **Parquet/Arrow**: Corruption or format incompatibility
- **JSON**: Invalid JSON syntax
- **Generic file errors**: Not found, permission, I/O errors, out of memory

### 2. Enhanced Error Display
- Errors are now shown in the DataTable with clear formatting
- File paths are extracted from SQL queries for context
- Error messages include both the problem (❌) and solution (→)
- Technical details are preserved at the end when helpful

### 3. Better File Opening
- Added try-catch around file opening from tree
- Failed files open in a new tab with error description
- Errors are logged for debugging

## Examples of Improved Error Messages

### Before:
```
Error
baba
```

### After:
```
❌ Cannot read CSV file: /path/to/file.csv
→ The file may be corrupted, empty, or in an unsupported format

Details: [technical details]
```

### Specific Error Types:

1. **Empty CSV**:
   ```
   ❌ Empty or invalid CSV file
   → The file appears to be empty or has no valid columns
   ```

2. **Format Error**:
   ```
   ❌ CSV format error
   → The file may have inconsistent delimiters. Check the CSV format
   ```

3. **File Not Found**:
   ```
   ❌ File not found: /path/to/file.csv
   → Check if the file exists and the path is correct
   ```

4. **Permission Error**:
   ```
   ❌ Permission denied: /path/to/file.csv
   → Check file permissions
   ```

## Testing

Run the test script to see error messages in action:
```bash
bash test_error_messages.sh
```

This creates test files with various issues:
- Empty CSV files
- CSV with inconsistent columns
- Fake CSV files (wrong content)
- Corrupted/binary CSV files

## Benefits

1. **Clear**: Immediately shows what went wrong
2. **Actionable**: Suggests how to fix the issue
3. **Specific**: Different messages for different error types
4. **Professional**: Uses icons and formatting for readability
5. **Debuggable**: Still includes technical details when needed
