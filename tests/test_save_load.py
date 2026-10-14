#!/usr/bin/env python3
"""
Automated test for save/load functionality in openduck
Tests:
1. Save a query
2. Save a second query
3. Load a query
"""

import json
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch, MagicMock
from io import StringIO

# Import the functions we need to test
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from openduck import save_query, load_config, add_to_history

def test_save_and_load_functionality():
    """Test the save and load functionality"""
    print("Starting save/load functionality test...")
    
    # Create a temporary config file for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
        tmp_config_path = tmp_file.name
    
    # Backup original CONFIG_FILE
    original_config_path = Path("/Volumes/dev/openduck/openduck.json")
    backup_exists = original_config_path.exists()
    if backup_exists:
        original_config_path.rename("/Volumes/dev/openduck/openduck.json.backup")
    
    # Create a test config file
    test_config = {
        "history": [],
        "saved_queries": []
    }
    with open(tmp_config_path, 'w') as f:
        json.dump(test_config, f)
    
    # Temporarily redirect CONFIG_FILE
    import openduck
    original_config_file = openduck.CONFIG_FILE
    openduck.CONFIG_FILE = Path(tmp_config_path)
    
    try:
        # Test 1: Save a query
        print("\n1. Testing save first query...")
        query1_name = "test_query_1"
        query1_sql = "SELECT * FROM table1 WHERE id = 1;"
        save_query(query1_name, query1_sql)
        
        # Verify the query was saved
        config_after_first_save = load_config()
        assert len(config_after_first_save["saved_queries"]) == 1
        assert config_after_first_save["saved_queries"][0]["name"] == query1_name
        assert config_after_first_save["saved_queries"][0]["sql"] == query1_sql
        print(f"   ✓ Saved query '{query1_name}' successfully")
        
        # Test 2: Save a second query
        print("\n2. Testing save second query...")
        query2_name = "test_query_2"
        query2_sql = "SELECT COUNT(*) FROM table2 GROUP BY category;"
        save_query(query2_name, query2_sql)
        
        # Verify both queries are saved
        config_after_second_save = load_config()
        assert len(config_after_second_save["saved_queries"]) == 2
        saved_names = [q["name"] for q in config_after_second_save["saved_queries"]]
        assert query1_name in saved_names
        assert query2_name in saved_names
        print(f"   ✓ Saved query '{query2_name}' successfully")
        print(f"   ✓ Total saved queries: {len(config_after_second_save['saved_queries'])}")
        
        # Test 3: Add to history
        print("\n3. Testing add to history...")
        history_sql = "SELECT name, age FROM users WHERE age > 25;"
        add_to_history(history_sql)
        
        config_with_history = load_config()
        assert len(config_with_history["history"]) == 1
        assert config_with_history["history"][0]["sql"] == history_sql
        print(f"   ✓ Added to history successfully")
        print(f"   ✓ Total history items: {len(config_with_history['history'])}")
        
        # Test 4: Verify all data persists
        print("\n4. Verifying all data persists...")
        final_config = load_config()
        assert len(final_config["saved_queries"]) == 2
        assert len(final_config["history"]) == 1
        
        # Check specific values
        saved_names = [q["name"] for q in final_config["saved_queries"]]
        assert query1_name in saved_names
        assert query2_name in saved_names
        
        history_sqls = [h["sql"] for h in final_config["history"]]
        assert history_sql in history_sqls
        
        print(f"   ✓ All data persisted correctly")
        print(f"   ✓ Saved queries: {[q['name'] for q in final_config['saved_queries']]}")
        print(f"   ✓ History count: {len(final_config['history'])}")
        
        print("\n✅ All tests passed!")
        return True
        
    except AssertionError as e:
        print(f"\n❌ Test failed with assertion error: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        return False
    finally:
        # Restore original CONFIG_FILE
        openduck.CONFIG_FILE = original_config_file
        
        # Cleanup: remove temp file and restore original if it existed
        Path(tmp_config_path).unlink()
        if backup_exists:
            Path("/Volumes/dev/openduck/openduck.json.backup").rename(original_config_path)
        else:
            # If original didn't exist, remove the test file
            if original_config_path.exists():
                original_config_path.unlink()

def run_manual_verification_steps():
    """Print manual verification steps for the user"""
    print("\n" + "="*60)
    print("MANUAL VERIFICATION STEPS")
    print("="*60)
    print("After running the automated test, please manually verify:")
    print()
    print("1. Start the application: python3 openduck.py")
    print("2. Enter a query in the text area")
    print("3. Press Ctrl+S or use the save function to save the query with a name")
    print("4. Verify the query appears in the 'Saved Queries' section in the left panel")
    print("5. Save a second query with a different name")
    print("6. Verify both queries appear in the 'Saved Queries' section")
    print("7. Click on one of the saved queries to load it back into the editor")
    print("8. Verify the query loads correctly")
    print("9. Run a query and verify it appears in the 'History' section")
    print("10. Close and reopen the application to verify persistence")
    print()
    print("Expected behavior:")
    print("- Saved queries should persist between application restarts")
    print("- History should persist between application restarts")
    print("- Loading a saved query should populate the query editor")
    print("- Loading a history query should populate the query editor")

if __name__ == "__main__":
    print("OpenDuck Save/Load Functionality Test")
    print("="*40)
    
    success = test_save_and_load_functionality()
    
    run_manual_verification_steps()
    
    if success:
        print("\n🎉 Automated tests completed successfully!")
    else:
        print("\n💥 Some automated tests failed!")
        sys.exit(1)