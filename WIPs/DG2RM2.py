import sqlite3
import json
import os
import logging
import re


def setup_logging():
    # Set up logging if not already configured
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG)

        # File handler for errors and warnings
        file_handler = logging.FileHandler('DG2RM2.log')
        file_handler.setLevel(logging.WARNING)  # Set to WARNING to capture warnings and errors
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)

        # Console handler for info messages
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)


setup_logging()

# Define global database paths
DNAGEDCOM_DB_PATH = r"F:\DNAGedcomRob.db"
ROOTSMAGIC_DB_PATH = r"F:\DNAGedcomRob - Copy.rmtree"


# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")


def connect_to_db(db_path, db_name):
    # Connect to the specified database
    try:
        conn = sqlite3.connect(db_path)
        if db_name == "RootsMagic":
            conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        logging.info(f"Connected to {db_name} database at: {db_path}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to {db_name} database: {e}")
        return None


def load_ethnicity_mappings():
    # Load ethnicity mappings from a JSON file
    script_dir = os.path.dirname(__file__)
    json_path = os.path.join(script_dir, "ethnicities.json")
    with open(json_path, 'r') as f:
        return json.load(f)


def insert_or_update_dna_kit_fact_type(cursor_rm):
    # Insert or update the "DNA Kit" fact type in RootsMagic database
    try:
        cursor_rm.execute("""
            INSERT OR IGNORE INTO FactTypeTable (OwnerType, Name, UseValue, Abbrev, GedcomTag, Sentence)
            VALUES (0, 'DNA Kit', 1, 'DNA_Kit', 'EVEN', '[person] had a DNA test performed. Kit #:< [Desc]>.')
        """)
        logging.info("Inserted 'DNA Kit' into FactTypeTable if it did not exist.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting 'DNA Kit' into FactTypeTable: {e}")


def user_kit_data(conn_dg):
    # Pull DNA_Kits and Ancestry_Profiles tables from DNAGedcom database
    dna_kits = []
    cursor_dg = None
    try:
        cursor_dg = conn_dg.cursor()
        cursor_dg.execute("SELECT guid, name FROM Ancestry_Profiles")
        dna_kits.extend(
            [(2, guid, *(name.rsplit(' ', 1) if ' ' in name else (name, ''))) for guid, name in cursor_dg.fetchall()])
        cursor_dg.execute("SELECT company, guid, name FROM DNA_Kits WHERE company IN ('FTDNA', 'MyHeritage')")
        for company, guid, name in cursor_dg.fetchall():
            if company == 'MyHeritage':
                guid = guid.replace('dnakit-', '')
                name = re.sub(r'\(.*\)', '', name).strip()
            dna_kits.append(
                (5 if company == 'MyHeritage' else 3, guid, *(name.rsplit(' ', 1) if ' ' in name else (name, ''))))
        logging.info("Extracted user kit ancestry_data successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error extracting user kit ancestry_data: {e}")
    finally:
        if cursor_dg:
            cursor_dg.close()
    return dna_kits


def process_and_insert_user_data(conn_rm, dna_kits):
    # Process and insert user ancestry_data into RootsMagic database
    cursor_rm = None
    try:
        cursor_rm = conn_rm.cursor()
        for company_id, guid, given_name, surname in dna_kits:
            cursor_rm.execute("SELECT OwnerID FROM NameTable WHERE Given = ? AND Surname = ?", (given_name, surname))
            person = cursor_rm.fetchone()
            if person:
                person_id = person[0]
            else:
                cursor_rm.execute("INSERT OR REPLACE INTO PersonTable (Sex) VALUES (2)")
                person_id = cursor_rm.lastrowid
                cursor_rm.execute("""
                    INSERT OR REPLACE INTO NameTable (OwnerID, Given, Surname, IsPrimary)
                    VALUES (?, ?, ?, 1)
                """, (person_id, given_name, surname))
                logging.info(f"Created record for '{given_name} {surname}' in NameTable.")

            if person_id:
                cursor_rm.execute("""
                    INSERT OR REPLACE INTO EventTable (OwnerID, OwnerType, EventType, Details)
                    SELECT ?, 0, (SELECT FactTypeID FROM FactTypeTable WHERE Name LIKE '%DNA Kit%'), ?
                    WHERE NOT EXISTS (SELECT 1 FROM EventTable WHERE Details = ?)
                """, (person_id, guid, guid))
                logging.info(f"Create or Update DNA Kit for '{given_name} {surname}' for '{guid}'.")
                conn_rm.commit()
            else:
                logging.error(f"Failed to Create or Update DNA Kit for '{given_name} {surname}' for '{guid}'.")
    except sqlite3.Error as e:
        logging.error(f"Error processing and inserting ancestry_data: {e}")
    finally:
        if cursor_rm:
            cursor_rm.close()


def ancestry_import(conn_dg, conn_rm):
    # Fetch and process ancestry_data from Ancestry tables in DNAGedcom
    cursor_dg = None
    cursor_rm = None
    try:
        cursor_dg = conn_dg.cursor()
        cursor_dg.execute("SELECT * FROM Ancestry_MatchGroups")
        rows = cursor_dg.fetchall()
        for row in rows:
            test_guid, match_guid, group_name, match_test_display_name = row[1], row[2], row[6], row[3]
            created_date = row[22].split()[0] if row[22] else ''
            match_tree_id, shared_cm = row[20], int(row[8])
            shared_percent, shared_segments = round((shared_cm / 7400) * 100, 2), row[12]
            cursor_rm = conn_rm.cursor()
            cursor_rm.execute("SELECT OwnerID FROM EventTable WHERE Details = ?", (test_guid,))
            result = cursor_rm.fetchone()
            if not result:
                logging.warning(f"No matching record found in EventTable for test_guid: {test_guid}")
                continue
            kit_owner_id = result[0]
            cursor_rm.execute("INSERT OR REPLACE INTO PersonTable (Sex) VALUES (2)")
            owner_id = cursor_rm.lastrowid
            given_name, surname = match_test_display_name.rsplit(' ', 1) if ' ' in match_test_display_name else (
                match_test_display_name, '')
            cursor_rm.execute("""
                INSERT OR REPLACE INTO NameTable (OwnerID, Given, Surname, IsPrimary)
                VALUES (?, ?, ?, 1)
            """, (owner_id, given_name, surname))
            cursor_rm.execute("""
                INSERT OR REPLACE INTO DNATable (RecID, DNAProvider, ID1, ID2, Label1, Label2, Date, Note, SharedCM, 
                                                    SharedPercent, SharedSegs)
                VALUES (?, 2, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (owner_id, kit_owner_id, owner_id, group_name, match_test_display_name, created_date, match_tree_id,
                  shared_cm, shared_percent, shared_segments))
            cursor_rm.execute("""
                INSERT OR REPLACE INTO EventTable (OwnerID, OwnerType, EventType, Details)
                SELECT ?, 0, (SELECT FactTypeID FROM FactTypeTable WHERE Name LIKE '%DNA Kit%'), ?
                WHERE NOT EXISTS (SELECT 1 FROM EventTable WHERE Details = ?)
            """, (owner_id, match_guid, match_guid))
            logging.info(f"Create or Update DNA Kit for '{given_name} {surname}' for '{match_guid}'.")
            conn_rm.commit()
    except sqlite3.Error as e:
        logging.error(f"Error importing ancestry_data from Ancestry tables: {e}")
    finally:
        if cursor_dg:
            cursor_dg.close()
        if cursor_rm:
            cursor_rm.close()


if __name__ == "__main__":
    # Main script execution
    rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, "RootsMagic")
    if rootsmagic_conn:
        insert_or_update_dna_kit_fact_type(rootsmagic_conn.cursor())
        dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, "DNAGedcom")
        if dnagedcom_conn:
            dna_kits_data = user_kit_data(dnagedcom_conn)
            if dna_kits_data:
                process_and_insert_user_data(rootsmagic_conn, dna_kits_data)
            ancestry_import(dnagedcom_conn, rootsmagic_conn)
            dnagedcom_conn.close()
        else:
            logging.error("Unable to establish database connection to DNAGedcom. Aborting.")
        rootsmagic_conn.close()
    else:
        logging.error("Unable to establish database connection to RootsMagic. Aborting.")
