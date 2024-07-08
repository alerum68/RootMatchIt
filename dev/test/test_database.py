import os
import sqlite3
import pytest
from functions.database import connect_to_db, DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH


@pytest.fixture
def setup_db_files(tmpdir):
    # Create temporary database files
    dgedcom_db = tmpdir.join("test_dgedcom.db")
    rootsmagic_db = tmpdir.join("test_rootsmagic.rmtree")

    # Create empty databases
    for db in (dgedcom_db, rootsmagic_db):
        conn = sqlite3.connect(db)
        conn.close()

    return str(dgedcom_db), str(rootsmagic_db)


def test_connect_to_dgedcom_db(setup_db_files):
    dgedcom_db, _ = setup_db_files
    conn = connect_to_db(dgedcom_db, "DNAGedcom")
    assert conn is not None
    conn.close()


def test_connect_to_rootsmagic_db(setup_db_files):
    _, rootsmagic_db = setup_db_files
    conn = connect_to_db(rootsmagic_db, "RootsMagic")
    assert conn is not None

    # Test if RMNOCASE collation works
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (name TEXT COLLATE RMNOCASE)")
    cursor.execute("INSERT INTO test (name) VALUES ('abc')")
    cursor.execute("INSERT INTO test (name) VALUES ('ABC')")
    cursor.execute("SELECT * FROM test WHERE name = 'ABC'")
    results = cursor.fetchall()
    assert len(results) == 2  # Should find both 'abc' and 'ABC'
    conn.close()


def test_failed_connection():
    conn = connect_to_db("invalid/path/to/db", "InvalidDB")
    assert conn is None
