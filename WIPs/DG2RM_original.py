import sqlite3
import logging

# Setup logging
logging.basicConfig(filename='dna_to_rootsmagic.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define RMNOCase collation
def rmnocase_collation(x, y):
    return (x.lower() > y.lower()) - (x.lower() < y.lower())

# Connect to the databases
def connect_to_databases(dnagdb_path, rootsmagic_path):
    dnag_conn = sqlite3.connect(dnagdb_path)
    rootsmagic_conn = sqlite3.connect(rootsmagic_path)
    rootsmagic_conn.create_collation("RMNOCASE", rmnocase_collation)
    return dnag_conn, rootsmagic_conn

# Main function to run the script
def main():
    # Uncomment the lines below and comment out the input lines to use default database paths
    # dnagedcom_db_path = "C:\DNAGedCom.db"
    # rootsmagic_db_path = "C:\RootsMagic.rmtree"

    dnagedcom_db_path = input("Enter the path to the DNAGedcom database: ")
    rootsmagic_db_path = input("Enter the path to the RootsMagic database: ")

    # Connect to databases
    dnag_conn, rootsmagic_conn = connect_to_databases(dnagdb_path, rootsmagic_path)

    # Placeholder for processing functions
    # e.g., process_familytreedna(dnag_conn, rootsmagic_conn)

    # Close connections
    dnag_conn.close()
    rootsmagic_conn.close()

if __name__ == '__main__':
    main()
