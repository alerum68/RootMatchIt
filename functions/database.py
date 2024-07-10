# Local Imports
from setup_logging import setup_logging
# Remote Imports
import os
import logging
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


Base = declarative_base()


def init_db(database_url):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    dg_session = Session()
    rm_session = Session()
    return dg_session, rm_session


def find_database_paths():
    db_directory = "./db"
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
        dg_db_path = input("Enter the path to the DNAGedcom database: ")
        rm_db_path = input("Enter the path to the RootsMagic database: ")
    else:
        dg_db_path = dg_db_file
        rm_db_path = rm_db_file

    return dg_db_path, rm_db_path


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


def connect_to_db_sqlalchemy(dg_db_path, rm_db_path):
    try:
        # Connect to DNAGedcom database
        dg_engine = create_engine(f"sqlite:///{dg_db_path}")
        DGSession = sessionmaker(bind=dg_engine)
        dg_session = DGSession()

        # Connect to RootsMagic database
        rm_engine = create_engine(f"sqlite:///{rm_db_path}")
        RMSession = sessionmaker(bind=rm_engine)
        rm_session = RMSession()

        logging.info(f"Connected to DNAGedcom database at: {dg_db_path} using SQLAlchemy")
        logging.info(f"Connected to RootsMagic database at: {rm_db_path} using SQLAlchemy")

        return dg_session, rm_session
    except Exception as sa_e:
        logging.error(f"Error connecting to databases using SQLAlchemy: {sa_e}")
        return None, None


def main():
    setup_logging()

    # Find database paths
    DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH = find_database_paths()

    # Connect using sqlite3
    dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, db_name="DNAGedcom")
    rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, db_name="RootsMagic")

    # Connect using SQLAlchemy
    dnagedcom_session, dnagedcom_engine = connect_to_db_sqlalchemy(DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH)


if __name__ == "__main__":
    main()
