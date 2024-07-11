import pytest
from functions.database import connect_to_db_sqlalchemy
import sqlite3


@pytest.fixture
def setup_db_files(tmpdir):
    # Create temporary database files
    dgedcom_db = tmpdir.join("test_dgedcom.db")
    rootsmagic_db = tmpdir.join("test_rootsmagic.rmtree")

    # Create empty databases
    for db in (str(dgedcom_db), str(rootsmagic_db)):
        conn = sqlite3.connect(db)
        conn.close()

    return str(dgedcom_db), str(rootsmagic_db)


def test_connect_to_dgedcom_db(setup_db_files):
    dgedcom_db, _ = setup_db_files
    conn = sqlite3.connect(dgedcom_db)
    assert conn is not None
    conn.close()


def test_connect_to_rootsmagic_db(setup_db_files):
    _, rootsmagic_db = setup_db_files
    conn = sqlite3.connect(rootsmagic_db)

    # Define RMNOCASE collation if necessary
    try:
        conn.execute("CREATE TABLE test (name TEXT COLLATE RMNOCASE)")
    except sqlite3.OperationalError:
        conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        conn.execute("CREATE TABLE test (name TEXT COLLATE RMNOCASE)")

    # Perform tests on the table creation
    cursor = conn.cursor()
    cursor.execute("INSERT INTO test (name) VALUES ('abc')")
    cursor.execute("INSERT INTO test (name) VALUES ('ABC')")
    cursor.execute("SELECT * FROM test WHERE name = 'ABC'")
    results = cursor.fetchall()
    assert len(results) == 2  # Should find both 'abc' and 'ABC'
    conn.close()


def test_failed_connection():
    invalid_db_path = "/invalid_db"  # Replace with an intentionally invalid path

    conn = sqlite3.connect(invalid_db_path)
    assert conn is None  # Ensure connection fails

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM some_table")  # Attempt to execute a query (this should raise an exception)
        assert False, "Expected sqlite3.OperationalError to be raised"
    except sqlite3.OperationalError:
        pass  # Expected exception

    conn.close()


def test_connect_to_dgedcom_db_sqlalchemy(setup_db_files):
    dgedcom_db, _ = setup_db_files
    dg_session, _ = connect_to_db_sqlalchemy(dgedcom_db, "DNAGedcom")
    assert dg_session is not None
    dg_session.close()


def test_connect_to_rootsmagic_db_sqlalchemy(setup_db_files):
    _, rootsmagic_db = setup_db_files
    _, rm_session = connect_to_db_sqlalchemy("invalid/path/to/db", rootsmagic_db)
    assert rm_session is not None
