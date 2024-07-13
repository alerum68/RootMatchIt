# database.py
# Local Imports
from sqlalchemy.event import listen

from setup_logging import setup_logging

# Remote Imports
import os
import logging
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


def init_db(database_url):
    # Initialize the database by creating tables based on the given database URL.
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    dg_session = Session()
    rm_session = Session()
    return dg_session, rm_session


def rmnocase_collation(str1, str2):
    return (str1.lower() > str2.lower()) - (str1.lower() < str2.lower())

def add_collation(dbapi_conn, _):
    dbapi_conn.create_collation('RMNOCASE', rmnocase_collation)


def find_database_paths():
    # Find the paths to the DNAGedcom and RootsMagic databases either from a directory or user input.
    db_directory = ".\\db"
    dg_db_file = None
    rm_db_file = None

    if os.path.exists(db_directory):
        files = os.listdir(db_directory)
        for file in files:
            if file.endswith(".db"):
                dg_db_file = os.path.join(db_directory, file)
            elif file.endswith(".rmtree"):
                rm_db_file = os.path.join(db_directory, file)

    if not dg_db_file or not rm_db_file:
        # If no databases found in directory, fall back to manual input
        print("No database files found in ./db directory.")
        dg_db_path = input("Enter the path to the DNAGedcom database: ").strip()
        rm_db_path = input("Enter the path to the RootsMagic database: ").strip()
    else:
        dg_db_path = dg_db_file
        rm_db_path = rm_db_file

    return dg_db_path, rm_db_path


def connect_to_db(db_path, db_name=None):
    # Connect to a SQLite database using sqlite3.
    try:
        conn = sqlite3.connect(db_path)
        if db_name == "RootsMagic":
            conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        logging.info(f"Connected to {db_name or 'database'} database at: {db_path}")
        return conn
    except sqlite3.Error as s3_e:
        logging.error(f"Error connecting to {db_name or 'database'} database: {s3_e}")
        return None


def connect_to_db_sqlalchemy(dg_db_path, rm_db_path):
    # Connect to a SQLite database using SqlAlchemy.
    try:
        # Connect to DNAGedcom database
        dg_engine = create_engine(f"sqlite:///{dg_db_path}")
        DGSession = sessionmaker(bind=dg_engine)
        dg_session = DGSession()
        logging.info(f"Connected to DNAGedcom database at: {dg_db_path} using SQLAlchemy")

        # Connect to RootsMagic database
        rm_engine = create_engine(f"sqlite:///{rm_db_path}")
        listen(rm_engine, 'connect', add_collation)
        RMSession = sessionmaker(bind=rm_engine)
        rm_session = RMSession()
        logging.info(f"Connected to RootsMagic database at: {rm_db_path} using SQLAlchemy")

        return dg_session, dg_engine, rm_session, rm_engine

    except Exception as sa_e:
        logging.error(f"Error connecting to databases using SQLAlchemy: {sa_e}")
        return None, None, None, None


def main():
    setup_logging()

    dnagedcom_conn = None
    rootsmagic_conn = None
    dnagedcom_session = None
    dnagedcom_engine = None
    rootsmagic_session = None
    rootsmagic_engine = None

    try:
        DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH = find_database_paths()

        # Connect using sqlite3
        dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, db_name="DNAGedcom")
        rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, db_name="RootsMagic")

        # Connect using SQLAlchemy
        dnagedcom_session, dnagedcom_engine, rootsmagic_session, rootsmagic_engine = connect_to_db_sqlalchemy(
            DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH)

    except Exception as e:
        logging.critical(f"Critical error in database connections: {e}")

    finally:
        if dnagedcom_conn:
            dnagedcom_conn.close()
        if rootsmagic_conn:
            rootsmagic_conn.close()

        if dnagedcom_session:
            dnagedcom_session.close()
        if dnagedcom_engine:
            dnagedcom_engine.dispose()
        if rootsmagic_session:
            rootsmagic_session.close()
        if rootsmagic_engine:
            rootsmagic_engine.dispose()

    logging.info("Closed connection to RootsMagic and DNAGedcom databases.")


if __name__ == "__main__":
    main()
