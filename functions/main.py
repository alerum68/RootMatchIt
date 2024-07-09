# local#
from setup_logging import setup_logging
from database import DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH, connect_to_db
import logging
# remote#
import logging

def main():
    # Main script execution
    setup_logging()
    rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, "RootsMagic")
    if rootsmagic_conn:
        # Proceed with your database operations
        dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, "DNAGedcom")
        if dnagedcom_conn:
            # Example of how to control function calls
            # sample_size = 50  # Set sample size here if needed
            # data = gather_data(dnagedcom_conn, sample_size)

            # Process data or import into RootsMagic as needed

            dnagedcom_conn.close()
        else:
            logging.error("Unable to establish database connection to DNAGedcom. Aborting.")
        rootsmagic_conn.close()
    else:
        logging.error("Unable to establish database connection to RootsMagic. Aborting.")


if __name__ == "__main__":
    main()
