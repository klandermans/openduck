"""Diagnostic classification for file-related query errors."""


def classify_file_error(e: Exception, file_path: str = "") -> str:
    """Classify file-related errors and return user-friendly message."""
    error_msg = str(e).lower()
    original_error = str(e)

    # CSV-specific errors
    if 'csv' in error_msg or 'read_csv' in error_msg:
        if 'no such file' in error_msg or 'could not open' in error_msg:
            return f"❌ File not found: {file_path}\n→ Check if the file exists and the path is correct"
        elif 'permission' in error_msg:
            return f"❌ Permission denied: {file_path}\n→ Check file permissions"
        elif 'encoding' in error_msg:
            return f"❌ Encoding error in CSV file\n→ The CSV file may have encoding issues. Try saving it with UTF-8 encoding"
        elif 'delimiter' in error_msg or 'separator' in error_msg:
            return f"❌ CSV format error\n→ The file may have inconsistent delimiters. Check the CSV format"
        elif 'empty' in error_msg or 'no columns' in error_msg or 'no rows' in error_msg:
            return f"❌ Empty or invalid CSV file\n→ The file appears to be empty or has no valid columns"
        else:
            return f"❌ Cannot read CSV file: {file_path}\n→ The file may be corrupted, empty, or in an unsupported format\n\nDetails: {str(e)}"

    # Excel-specific errors
    elif 'excel' in error_msg or 'xlsx' in error_msg or 'xls' in error_msg:
        if 'no such file' in error_msg:
            return f"❌ Excel file not found: {file_path}"
        elif 'permission' in error_msg:
            return f"❌ Permission denied: {file_path}"
        else:
            return f"❌ Cannot read Excel file: {file_path}\n→ The file may be corrupted, password-protected, or in an unsupported format\n\nDetails: {str(e)}"

    # Parquet/Arrow errors
    elif 'parquet' in error_msg or 'arrow' in error_msg:
        return f"❌ Cannot read Parquet/Arrow file: {file_path}\n→ The file may be corrupted or in an incompatible format\n\nDetails: {str(e)}"

    # JSON errors
    elif 'json' in error_msg:
        return f"❌ Cannot read JSON file: {file_path}\n→ The file may have invalid JSON syntax\n\nDetails: {str(e)}"

    # Generic file errors
    elif 'no such file' in error_msg or 'does not exist' in error_msg:
        return f"❌ File not found: {file_path}\n→ The file does not exist at this location"
    elif 'permission' in error_msg:
        return f"❌ Permission denied: {file_path}\n→ You don't have permission to access this file"
    elif 'too many open files' in error_msg or 'errno 24' in error_msg:
        return f"❌ Too many open files\n→ System file descriptor limit reached"
    elif 'disk i/o' in error_msg or 'io error' in error_msg:
        return f"❌ Disk I/O error\n→ There may be a problem with the disk or file system"
    elif 'out of memory' in error_msg:
        return f"❌ Out of memory\n→ The file is too large to load into memory"
    elif 'empty' in original_error.lower() or 'no columns' in original_error.lower():
        return f"❌ Empty or invalid file\n→ The file appears to be empty or has no valid data"
    else:
        return f"❌ Error loading file: {file_path}\n\nDetails: {str(e)}"
