import logging
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Define global database paths. Switch the comment and uncomment on the lines below to use hard-coded database paths.
# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")
DNAGEDCOM_DB_PATH = r"..\db\Alerum68.db"
ROOTSMAGIC_DB_PATH = r"..\db\Alerum68 - Copy.rmtree"


def init_db(database_url):
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    return Session


def connect_to_db(db_path, db_name=None):

    # Connect to databases, and create RMNOCASE collation in RootsMagic.
    try:
        conn = sqlite3.connect(db_path)
        if db_name == "RootsMagic":
            conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        logging.info(f"Connected to {db_name or 'database'} database at: {db_path}")
        return conn
    except sqlite3.Error as s3_e:
        logging.error(f"Error connecting to {db_name or 'database'} database: {s3_e}")
        return None


def connect_to_db_sqlalchemy(db_path, db_name=None):
    # Connect to databases using SQLAlchemy.
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info(f"Connected to {db_name or 'database'} database at: {db_path} using SQLAlchemy")
        return session, engine
    except Exception as sq_e:
        logging.error(f"Error connecting to {db_name or 'database'} database using SQLAlchemy: {sq_e}")
        return None, None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Connect using sqlite3
    dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, db_name="DNAGedcom")
    rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, db_name="RootsMagic")

    # Connect using SQLAlchemy
    dnagedcom_session, dnagedcom_engine = connect_to_db_sqlalchemy(DNAGEDCOM_DB_PATH, db_name="DNAGedcom")
    rootsmagic_session, rootsmagic_engine = connect_to_db_sqlalchemy(ROOTSMAGIC_DB_PATH, db_name="RootsMagic")
