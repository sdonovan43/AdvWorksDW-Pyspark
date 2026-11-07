"""
Test script to verify the ETL pipeline components
"""

import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'src'))
sys.path.append(os.path.join(os.getcwd(), 'utils'))

def test_imports():
    """Test that all modules can be imported successfully"""
    print("Testing module imports...")
    
    try:
        from src.extract import DataExtractor
        from src.transform import DataTransformer  
        from src.load import DataLoader
        from utils.db_utils import DatabaseConnector
        
        print("✓ All modules imported successfully")
        return True
    except Exception as e:
        print(f"✗ Import test failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of components"""
    print("\nTesting basic functionality...")
    
    try:
        # Test database connection (will fail if no connection)
        from utils.db_utils import DatabaseConnector
        db = DatabaseConnector()
        tables = db.get_all_table_names()
        
        if len(tables) > 0:
            print(f"✓ Database connection successful, found {len(tables)} tables")
            
            # Test extraction of a small table
            test_table = 'DimCustomer'
            if test_table in tables:
                extractor = DataExtractor(db)
                df = extractor.extract_table(test_table)
                print(f"✓ Successfully extracted {test_table}: {len(df)} rows")
                
            return True
        else:
            print("✗ No tables found in database")
            return False
            
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("AdventureWorksDW2022 Pipeline - Test Suite")
    print("=" * 50)
    
    # Run tests
    import_success = test_imports()
    functionality_success = test_basic_functionality()
    
    if import_success and functionality_success:
        print("\n✓ ALL TESTS PASSED")
        print("Pipeline components are working correctly!")
        return 0
    else:
        print("\n✗ SOME TESTS FAILED")  
        return 1

if __name__ == "__main__":
    sys.exit(main())