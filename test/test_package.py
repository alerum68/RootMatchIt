import pytest
import os
import ast
import astor
from package import extract_imports_and_base  # Importing extract_imports_and_base from your package

# Define setup_files with corrected relative paths
setup_files = [
    "../functions/setup_logging.py",  # Example corrected path
    "../functions/database.py",
    "../classes/rm_classes.py",
    "../classes/a_classes.py",
    "../classes/ftdna_classes.py",
    "../classes/mh_classes.py",
    "../functions/select_kits.py",
    "../functions/import_data.py",
    "../functions/process_data.py",
    "../functions/create_indices.py",
    "../functions/main.py"
]


@pytest.fixture
def setup_files_fixture():
    # Setup: Create temporary files for testing
    for fname in setup_files:
        with open(fname, "w") as f:
            f.write("# Placeholder content\n")

    yield setup_files  # Yield setup_files list

    # Teardown: Clean up temporary files
    for fname in setup_files:
        os.remove(fname)


def test_extract_imports_and_base(setup_files_fixture):
    global imported_base  # Ensure imported_base is treated as global within the test function

    # Test extract_imports_and_base function
    import_lines = set()  # Use a local set for import_lines within the test function
    imported_base = False  # Reset imported_base for each test case

    for fname in setup_files_fixture:
        extract_imports_and_base(fname)

    # Assertions
    expected_imports = {
        "import ast",
        "import os",
        "import astor",
        "from sqlalchemy.ext.declarative import declarative_base as Ancestry_Base"
        # Add expected imports based on your script logic
    }

    assert import_lines == expected_imports, "Extracted imports do not match expected"

    # Check if base assignment was imported
    assert imported_base, "Ancestry_Base assignment from SQLAlchemy was not imported"

# You can add more tests as needed
