import logging
import sqlite3


# Define global database paths. Switch the comment and uncomment on the lines below to use hard-coded database paths.
# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")
DNAGEDCOM_DB_PATH = r"db\Alerum68.db"
ROOTSMAGIC_DB_PATH = r"db\Alerum68 - Copy.rmtree"


def connect_to_db(db_path, db_name=None):
    # Connect to databases, and create RMNOCASE collation in RootsMagic.
    try:
        conn = sqlite3.connect(db_path)
        if db_name == "RootsMagic":
            conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        logging.info(f"Connected to {db_name or 'database'} database at: {db_path}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to {db_name or 'database'} database: {e}")
        return None
