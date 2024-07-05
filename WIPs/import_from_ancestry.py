import sqlite3
import json
import os
import logging
# import re
# import sys

# Define global database paths
DNAGEDCOM_DB_PATH = r"F:\alerum68.db"
ROOTSMAGIC_DB_PATH = r"F:\alerum68 - Copy.rmtree"


# Uncomment the lines below to take paths as user input
# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")


def setup_logging():
    # Set up logging
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG)

        # File handler for errors and warnings
        file_handler = logging.FileHandler('DG2RM2_error.log')
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)

        # File handler for info and debug
        file_handler = logging.FileHandler('DG2RM2.log')
        file_handler.setLevel(logging.DEBUG)  # Set to DEBUG level for all messages
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)

        # Console handler for info and debug
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)  # Set to DEBUG level for all messages
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)


def connect_to_db(db_path, db_name=None):
    try:
        conn = sqlite3.connect(db_path)
        if db_name == "RootsMagic":
            conn.create_collation("RMNOCASE", lambda x, y: (x.lower() > y.lower()) - (x.lower() < y.lower()))
        print(f"Connected to {db_name or 'database'} database at: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to {db_name or 'database'} database: {e}")
        return None


def load_ethnicity_mappings():
    # Load ethnicity mappings from a JSON file
    script_dir = os.path.dirname(__file__)
    json_path = os.path.join(script_dir, "ethnicities.json")
    with open(json_path, 'r') as f:
        return json.load(f)


def dna_kit_fact_type(cursor_rm):
    # Insert or update the "DNA Kit" fact type in RootsMagic database
    try:
        cursor_rm.execute("""
            INSERT INTO FactTypeTable (OwnerType, Name, UseValue, Abbrev, GedcomTag, Sentence)
            SELECT 0, 'DNA Kit', 1, 'DNA Kit', 'EVEN', '[person] had a DNA test performed. Kit #:< [Desc]>.'
            WHERE NOT EXISTS (SELECT 1 FROM FactTypeTable WHERE Name = 'DNA Kit')
        """)
        if cursor_rm.rowcount > 0:
            logging.info("Inserted 'DNA Kit' into FactTypeTable.")
        else:
            logging.info("'DNA Kit' already exists in FactTypeTable. Skipped insertion.")
        cursor_rm.connection.commit()
    except sqlite3.Error as e:
        logging.error(f"Error inserting or updating 'DNA Kit' in FactTypeTable: {e}")


def gather_data(conn_dg, sample_size=None):
    data = {}
    cursor_dg = conn_dg.cursor()

    try:
        tables = {
            'Ancestry': ["Ancestry_ICW", "Ancestry_matchTrees", "Ancestry_matchGroups", "Ancestry_Profiles"]
        }

        for provider, table_list in tables.items():
            data[provider] = {}
            for table in table_list:
                query = f"SELECT * FROM {table}"
                if sample_size:
                    query += f" LIMIT {sample_size}"
                cursor_dg.execute(query)
                rows = cursor_dg.fetchall()
                headers = [description[0] for description in cursor_dg.description]
                data[provider][table] = [dict(zip(headers, row)) for row in rows]

        print("Data dictionary contents:")
        print(data)

    except sqlite3.Error as e:
        print(f"Error gathering data: {e}")
    finally:
        cursor_dg.close()

    return data


'''def process_user_kits(conn_rm, data):
    kits = []
    idx = 1

    def display_and_collect_kits(provider, profiles, company_id=None):
        nonlocal idx
        print(f"{provider}:")
        for profile in profiles:
            name = profile.get('name', '')
            print(f"{idx}. {name}")
            kits.append((company_id or provider, profile))
            idx += 1

    # Display profiles
    display_and_collect_kits("Ancestry_Profiles", data['Profiles'].get('Ancestry_Profiles', []))

    # Process DNA kits by provider
    ftdna_kits = [kit for kit in data['Profiles'].get('DNA_Kits', []) if kit['company'] == 'FTDNA']
    myheritage_kits = [kit for kit in data['Profiles'].get('DNA_Kits', []) if kit['company'] == 'MyHeritage']

    display_and_collect_kits("FTDNA", ftdna_kits, 'FTDNA')
    display_and_collect_kits("MyHeritage", myheritage_kits, 'MyHeritage')

    selected_indices = input(
        "Enter the numbers of the kits you want to select (a for Ancestry, f for FTDNA, h for MyHeritage, or all): "
    ).lower().strip()

    if selected_indices == 'all':
        selected_indices = list(range(1, len(kits) + 1))
    elif selected_indices in ['a', 'f', 'h']:
        provider_map = {'a': 'Ancestry_Profiles', 'f': 'FTDNA', 'h': 'MyHeritage'}
        selected_indices = [i + 1 for i, (provider, _) in enumerate(kits) if provider == provider_map[selected_indices]]
    else:
        try:
            selected_indices = [int(i.strip()) for i in selected_indices.split(',')]
        except ValueError:
            print("Invalid input. Please enter numbers only.")
            sys.exit(1)

    selected_kits = [kits[i - 1] for i in selected_indices if 1 <= i <= len(kits)]

    cursor_rm = None
    try:
        cursor_rm = conn_rm.cursor()
        dna_kits = []
        for provider, profile in selected_kits:
            guid = profile.get('guid', '')
            name = profile.get('name', '')
            given_name, surname = name.split(' ', 1) if ' ' in name else (name, '')
            company_id = 5 if provider == 'MyHeritage' else 3 if provider == 'FTDNA' else 2
            dna_kits.append((company_id, guid, given_name, surname))
        import_rm(conn_rm, dna_kits)
    except sqlite3.Error as e:
        logging.error(f"Error processing and inserting data: {e}")
    finally:
        if cursor_rm:
            cursor_rm.close()'''


def process_ancestry(data):
    standardized_data_icw = []
    standardized_data_match_trees = []
    standardized_data_match_groups = []
    standardized_data_profiles = []

    def get_value(row, key, default=None):
        try:
            return row[key]
        except KeyError:
            logging.warning(f"Missing key '{key}' in row. Using default value '{default}'.")
            return default

    try:
        # Data inspection logging
        logging.info(f"Data inspection - Ancestry_ICW: {data.get('Ancestry_ICW', [])[:5]}")
        logging.info(f"Data inspection - Ancestry_matchTrees: {data.get('Ancestry_matchTrees', [])[:5]}")
        logging.info(f"Data inspection - Ancestry_matchGroups: {data.get('Ancestry_matchGroups', [])[:5]}")
        logging.info(f"Data inspection - Ancestry_Profiles: {data.get('Ancestry_Profiles', [])[:5]}")

        # Process Ancestry_ICW data
        for row in data.get('Ancestry_ICW', []):
            standardized_row = {
                'ID1': get_value(row, 'matchid'),
                'ID2': get_value(row, 'icwid'),
                'Date': get_value(row, 'created_date'),
                'sharedCM': int(get_value(row, 'sharedCentimorgans', 0)),
                'confidence': float(get_value(row, 'confidence', 0.0)),
                'SharedSegs': int(get_value(row, 'numSharedSegments', 0)),
                'MatchName': get_value(row, 'matchname'),
                'MatchAdmin': get_value(row, 'matchadmin'),
                'ICWName': get_value(row, 'icwname'),
                'ICWAdmin': get_value(row, 'icwadmin'),
                'Confidence': get_value(row, 'confidence'),
                'Meiosis': get_value(row, 'meiosisValue')
            }
            standardized_data_icw.append(standardized_row)

        logging.info(f"Ancestry ICW data standardized successfully: {standardized_data_icw}")

        # Process Ancestry_matchTrees data
        for row in data.get('Ancestry_matchTrees', []):
            standardized_row = {
                'matchGuid': get_value(row, 'matchGuid'),
                'Surname': get_value(row, 'surname'),
                'Given': get_value(row, 'given'),
                'BirthDate': get_value(row, 'birthDate'),
                'DeathDate': get_value(row, 'deathDate'),
                'BirthPlace': get_value(row, 'birthPlace'),
                'DeathPlace': get_value(row, 'deathPlace'),
                'relid': get_value(row, 'relid'),
                'PersonId': get_value(row, 'personId'),
                'OwnerID': get_value(row, 'personId'),
                'fatherId': get_value(row, 'fatherId'),
                'motherId': get_value(row, 'motherId'),
                'source': get_value(row, 'source'),
                'created_date': get_value(row, 'created_date'),
                'loginUsername': get_value(row, 'loginUsername'),
                'sync': get_value(row, 'sync')
            }
            standardized_data_match_trees.append(standardized_row)

        logging.info(f"Ancestry matchTrees data standardized successfully: {standardized_data_match_trees}")

        # Process Ancestry_matchGroups data
        for row in data.get('Ancestry_matchGroups', []):
            match_display_name = get_value(row, 'matchTestDisplayName', '')
            if ' ' in match_display_name:
                given_name, surname = match_display_name.rsplit(' ', 1)
            else:
                given_name = match_display_name
                surname = ''

            standardized_row = {
                'Id': get_value(row, 'Id'),
                'DNAProvider': "2",
                'Surname': surname,
                'Given': given_name,
                'ID1': get_value(row, 'testGuid'),
                'ID2': get_value(row, 'matchGuid'),
                'Group': get_value(row, 'groupName'),
                'SharedCM': get_value(row, 'sharedCentimorgans'),
                'SharedSegments': get_value(row, 'sharedSegment'),
                'LastLoggedInDate': get_value(row, 'lastLoggedInDate'),
                'Starred': get_value(row, 'starred'),
                'Viewed': get_value(row, 'viewed'),
                'MatchTreeIsPrivate': get_value(row, 'matchTreeIsPrivate'),
                'Note': get_value(row, 'note'),
                'UserPhoto': get_value(row, 'userPhoto'),
                'MatchTreeId': get_value(row, 'matchTreeId'),
                'TreeId': get_value(row, 'treeId'),
                'CreatedDate': get_value(row, 'created_date'),
                'LoginUsername': get_value(row, 'loginUsername'),
                'Sync': get_value(row, 'sync'),
                'Paternal': get_value(row, 'paternal'),
                'Maternal': get_value(row, 'maternal'),
                'Sex': get_value(row, 'subjectGender'),
                'MeiosisValue': get_value(row, 'meiosisValue'),
                'ParentCluster': get_value(row, 'parentCluster')
            }
            standardized_data_match_groups.append(standardized_row)

        logging.info(f"Ancestry matchGroups data standardized successfully: {standardized_data_match_groups}")

        # Process Ancestry_Profiles data
        for row in data.get('Ancestry_Profiles', []):
            standardized_row = {
                'guid': get_value(row, 'guid'),
                'name': get_value(row, 'name')
            }
            standardized_data_profiles.append(standardized_row)

        logging.info(f"Ancestry Profiles data standardized successfully: {standardized_data_profiles}")

    except KeyError as e:
        logging.error(f"Error standardizing Ancestry data: Missing key {e}")

    return (standardized_data_icw, standardized_data_match_trees,
            standardized_data_match_groups, standardized_data_profiles)


def process_ftdna(data):
    # Process and standardize FTDNA data
    standardized_data = []
    try:
        for row in data['FTDNA_Matches2']:
            standardized_row = {
                'Given': row['name'].split(' ')[0] if ' ' in row['name'] else row['name'],
                'Surname': row['name'].split(' ')[1] if ' ' in row['name'] else '',
                'MatchGuid': row['match_guid'],  # FTDNA_Matches2.match_guid
                'SharedCM': row['shared_cm'],  # FTDNA_Matches2.shared_cm
                'SharedSegments': row['shared_segments']  # FTDNA_Matches2.shared_segments
            }
            standardized_data.append(standardized_row)
        logging.info("FTDNA data standardized successfully.")
    except KeyError as e:
        logging.error(f"Error standardizing FTDNA data: Missing key {e}")
    return standardized_data


def process_mh(data):
    # Process and standardize MyHeritage data
    standardized_data = []
    try:
        for row in data['MH_Match']:
            standardized_row = {
                'Given': row['name'].split(' ')[0] if ' ' in row['name'] else row['name'],
                'Surname': row['name'].split(' ')[1] if ' ' in row['name'] else '',
                'MatchGuid': row['match_guid'],  # MH_Match.match_guid
                'SharedCM': row['shared_cm'],  # MH_Match.shared_cm
                'SharedSegments': row['shared_segments']  # MH_Match.shared_segments
            }
            standardized_data.append(standardized_row)
        logging.info("MyHeritage data standardized successfully.")
    except KeyError as e:
        logging.error(f"Error standardizing MyHeritage data: Missing key {e}")
    return standardized_data


def import_rm(conn_rm, dna_kits):
    # Import user DNA kits into RootsMagic database
    cursor_rm = None  # Initialize cursor_rm to None
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
        logging.error(f"Error importing data into RootsMagic database: {e}")
    finally:
        if cursor_rm:
            cursor_rm.close()
        # Do not close conn_rm here; it should be managed externally


"""def import_trees(conn_rm, tree_data):
    # Import family tree data into RootsMagic
    cursor_rm = None  # Initialize cursor_rm to None
    try:
        cursor_rm = conn_rm.cursor()
        for row in tree_data:
            # Implement the logic to import tree data
            ...
        logging.info("Family tree data imported successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error importing family tree data: {e}")
    finally:
        if cursor_rm:
            cursor_rm.close()
"""

if __name__ == "__main__":
    # Main script execution
    setup_logging()
    rootsmagic_conn = connect_to_db(ROOTSMAGIC_DB_PATH, "RootsMagic")
    if rootsmagic_conn:
        dna_kit_fact_type(rootsmagic_conn.cursor())
        dnagedcom_conn = connect_to_db(DNAGEDCOM_DB_PATH, "DNAGedcom")
        if dnagedcom_conn:
            # Example of how to control function calls
            sample_size = 50  # Set sample size here if needed
            data = gather_data(dnagedcom_conn, sample_size)

            # Process user data
            # process_user_kits(rootsmagic_conn, data)

            # Process data for each provider
            standardized_ancestry_data = process_ancestry(data['Ancestry'])
            # standardized_ftdna_data = process_ftdna(data['FTDNA'])
            # standardized_mh_data = process_mh(data['MyHeritage'])

            # Import standardized data into RootsMagic
            # import_rm(rootsmagic_conn, standardized_ancestry_data)
            # import_rm(rootsmagic_conn, standardized_ftdna_data)
            # import_rm(rootsmagic_conn, standardized_mh_data)

            # Import family trees if needed
            # import_trees(rootsmagic_conn, data['Ancestry'])  # Adjust as necessary

            dnagedcom_conn.close()
        else:
            logging.error("Unable to establish database connection to DNAGedcom. Aborting.")
        rootsmagic_conn.close()
    else:
        logging.error("Unable to establish database connection to RootsMagic. Aborting.")
