import sqlite3
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import uuid

# Define global database paths. Switch the comment and uncomment on the lines below to use hard-coded database paths.
# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")
DNAGEDCOM_DB_PATH = r"Alerum68.db"
ROOTSMAGIC_DB_PATH = r"Alerum68 - Copy.rmtree"


def setup_logging():
    # Set up logging
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG)

        # Output for errors and warnings to 'DG2RM2_error.log'
        file_handler_error = logging.FileHandler('DG2RM2_error.log', encoding='utf-8')
        file_handler_error.setLevel(logging.WARNING)
        file_handler_error.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_error)

        # Output for info and debug to 'DG2RM.log'
        file_handler_info = RotatingFileHandler('DG2RM.log', maxBytes=1000000, backupCount=5, encoding='utf-8')
        file_handler_info.setLevel(logging.INFO)
        file_handler_info.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler_info)

        # Console output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)

        root_logger.info(f"\n\n Start of run - {datetime.now().strftime('(%Y-%m-%d, %H:%M:%S)')} \n")


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
            logging.info("DNA Kit fact type added successfully")
        else:
            logging.info("'DNA Kit' already exists in FactTypeTable.  Skipped.")
        cursor_rm.connection.commit()
    except sqlite3.Error as e:
        logging.error(f"Error adding DNA Kit fact type: {e}")


def gather_data(conn_dg, size=None):
    # Assigning what tables to insert into the data Dictionary
    local_data = {}
    cursor_dg = conn_dg.cursor()

    try:
        tables = {
            'Ancestry': ["AncestryAncestorCouple", "Ancestry_Ethnicity", "Ancestry_ICW", "Ancestry_matchEthnicity",
                         "Ancestry_matchGroups", "Ancestry_matchTrees", "Ancestry_Profiles", "Ancestry_TreeData"],
            'FTDNA': ["DNA_Kits", "FTDNA_Matches2", "FTDNA_ICW2", "DGIndividual", "DGTree"],
            'MyHeritage': ["DNA_Kits", "MH_Match", "MH_ICW", "MH_Ancestors", "MH_Tree"]
        }

        for provider, table_list in tables.items():
            local_data[provider] = {}
            for table in table_list:
                query = f"SELECT * FROM {table}"
                if size:
                    query += f" LIMIT {size}"
                cursor_dg.execute(query)
                rows = cursor_dg.fetchall()
                headers = [description[0] for description in cursor_dg.description]
                local_data[provider][table] = [dict(zip(headers, row)) for row in rows]

    except sqlite3.Error as e:
        logging.error(f"Error gathering ancestry_data: {e}")
    finally:
        cursor_dg.close()

    return local_data


def process_ancestry(ancestry_data):
    # Standardize Ancestry data to RootsMagic DB structure.
    AncestryAncestorCouple = []
    Ancestry_Ethnicity = []
    Ancestry_icw = []
    Ancestry_matchEthnicity = []
    Ancestry_matchGroups = []
    Ancestry_matchTrees = []
    Ancestry_profiles = []
    Ancestry_TreeData = []

    def get_value(a_row, key, default=None):
        try:
            return a_row[key]
        except KeyError:
            return default

    try:
        logging.debug(f"Data Dict: AncestryAncestorCouple: {ancestry_data.get('AncestryAncestorCouple', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_Ethnicity: {ancestry_data.get('Ancestry_Ethnicity', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_icw: {ancestry_data.get('Ancestry_ICW', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_matchEthnicity: {ancestry_data.get('Ancestry_matchEthnicity', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_matchGroups: {ancestry_data.get('Ancestry_matchGroups', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_matchTrees: {ancestry_data.get('Ancestry_matchTrees', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_Profiles: {ancestry_data.get('Ancestry_Profiles', [])[:5]}")
        logging.debug(f"Data Dict: Ancestry_TreeData: {ancestry_data.get('Ancestry_TreeData', [])[:5]}")

        # Process AncestryAncestorCouple table
        for row in ancestry_data.get('AncestryAncestorCouple', []):
            standardized_row = {
                'ID1': get_value(row, 'TestGuid'),
                'ID2': get_value(row, 'MatchGuid'),
                'FatherAmtGid': get_value(row, 'FatherAmtGid'),
                'FatherBigTreeGid': get_value(row, 'FatherBigTreeGid'),
                'FatherKinshipPathToSampleId': get_value(row, 'FatherKinshipPathToSampleId'),
                'FatherKinshipPathFromSampleToMatch': get_value(row, 'FatherKinshipPathFromSampleToMatch'),
                'FatherPotential': get_value(row, 'FatherPotential'),
                'FatherInMatchTree': get_value(row, 'FatherInMatchTree'),
                'FatherInBestContributorTree': get_value(row, 'FatherInBestContributorTree'),
                'FatherDisplayName': get_value(row, 'FatherDisplayName'),
                'FatherBirthYear': get_value(row, 'FatherBirthYear'),
                'FatherDeathYear': get_value(row, 'FatherDeathYear'),
                'FatherIsMale': get_value(row, 'FatherIsMale'),
                'FatherNotFound': get_value(row, 'FatherNotFound'),
                'FatherVeiled': get_value(row, 'FatherVeiled'),
                'FatherRelationshipToSampleId': get_value(row, 'FatherRelationshipToSampleId'),
                'FatherRelationshipFromSampleToMatch': get_value(row, 'FatherRelationshipFromSampleToMatch'),
                'MotherAmtGid': get_value(row, 'MotherAmtGid'),
                'MotherBigTreeGid': get_value(row, 'MotherBigTreeGid'),
                'MotherKinshipPathToSampleId': get_value(row, 'MotherKinshipPathToSampleId'),
                'MotherKinshipPathFromSampleToMatch': get_value(row, 'MotherKinshipPathFromSampleToMatch'),
                'MotherPotential': get_value(row, 'MotherPotential'),
                'MotherInMatchTree': get_value(row, 'MotherInMatchTree'),
                'MotherInBestContributorTree': get_value(row, 'MotherInBestContributorTree'),
                'MotherDisplayName': get_value(row, 'MotherDisplayName'),
                'MotherBirthYear': get_value(row, 'MotherBirthYear'),
                'MotherDeathYear': get_value(row, 'MotherDeathYear'),
                'MotherIsFemale': get_value(row, 'MotherIsFemale'),
                'MotherNotFound': get_value(row, 'MotherNotFound'),
                'MotherVeiled': get_value(row, 'MotherVeiled'),
                'MotherRelationshipToSampleId': get_value(row, 'MotherRelationshipToSampleId'),
                'MotherRelationshipFromSampleToMatch': get_value(row, 'MotherRelationshipFromSampleToMatch'),
            }
            AncestryAncestorCouple.append(standardized_row)

        logging.info(f"AncestryAncestorCouple table standardized successfully: {AncestryAncestorCouple[:5]}" )

        # Process Ancestry_Ethnicity table
        for row in ancestry_data.get('Ancestry_Ethnicity', []):
            standardized_row = {
                'code': get_value(row, 'code'),
                'value': get_value(row, 'value'),
            }
            Ancestry_Ethnicity.append(standardized_row)

        logging.info(f"Ancestry_Ethnicity table standardized successfully: {Ancestry_Ethnicity[:5]}")

        # Process Ancestry_ICW table
        for row in ancestry_data.get('Ancestry_ICW', []):
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
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
            Ancestry_icw.append(standardized_row)

        logging.info(f"Ancestry_ICW table standardized successfully: {Ancestry_icw[:5]}")

        # Process Ancestry_matchGroups table
        for row in ancestry_data.get('Ancestry_matchGroups', []):
            match_display_name = get_value(row, 'matchTestDisplayName', '')
            if ' ' in match_display_name:
                given_name, surname = match_display_name.rsplit(' ', 1)
            else:
                given_name = match_display_name
                surname = ''

            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
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
            Ancestry_matchGroups.append(standardized_row)

        logging.info(f"Ancestry_matchGroups table standardized successfully: {Ancestry_matchGroups[:5]}")

        # Process Ancestry_matchEthnicity table
        for row in ancestry_data.get('Ancestry_matchEthnicity', []):
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
            Ancestry_matchEthnicity.append(standardized_row)

        logging.info(f"Ancestry_matchEthnicity table standardized successfully: {Ancestry_matchEthnicity[:5]}")

        # Process Ancestry_matchTrees table
        for row in ancestry_data.get('Ancestry_matchTrees', []):
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
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
            Ancestry_matchTrees.append(standardized_row)

        logging.info(f"Ancestry matchTrees table standardized successfully: {Ancestry_matchTrees[:5]}")

        # Process Ancestry_Profiles table
        for row in ancestry_data.get('Ancestry_Profiles', []):
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
                'guid': get_value(row, 'guid'),
                'name': get_value(row, 'name')
            }
            Ancestry_profiles.append(standardized_row)

        logging.info(f"Ancestry Profiles table standardized successfully: {Ancestry_profiles[:5]}")

        # Process Ancestry_TreeData table
        for row in ancestry_data.get('Ancestry_TreeData', []):
            standardized_row = {
                'guid': get_value(row, 'guid'),
                'name': get_value(row, 'name')
            }
            Ancestry_TreeData.append(standardized_row)

        logging.info(f"Ancestry_TreeData table standardized successfully: {Ancestry_TreeData[:5]}")

    except KeyError as e:
        logging.error(f"Error standardizing Ancestry_data: Missing key {e}")

    return (AncestryAncestorCouple, Ancestry_Ethnicity, Ancestry_icw, Ancestry_matchTrees,
            Ancestry_matchGroups, Ancestry_profiles)


def process_ftdna(ftdna_data):
    # Standardize FTDNA data to RootsMagic DB structure.
    DGIndividual = []
    DGTree = []
    DNA_Kits = []
    FTDNA_ICW2 = []
    FTDNA_Matches2 = []

    def get_value(f_row, key, default=None):
        try:
            return f_row[key]
        except KeyError:
            return default

    try:
        logging.debug(f"Data Dict: DGIndividual: {ftdna_data.get('DGIndividual', [])[:5]}")
        logging.debug(f"Data Dict: DGTree: {ftdna_data.get('DGTree', [])[:5]}")
        logging.debug(f"Data Dict: DNA_Kits: {ftdna_data.get('DNA_Kits', [])[:5]}")
        logging.debug(f"Data Dict: FTDNA_ICW2: {ftdna_data.get('FTDNA_ICW2', [])[:5]}")
        logging.debug(f"Data Dict: FTDNA_Matches2: {ftdna_data.get('FTDNA_Matches2', [])[:5]}")

        # Process FTDNA DGIndividual table
        for row in ftdna_data['DGIndividual']:
            standardized_row = {
                'Id': get_value(row, 'Id'),
                'treeid': get_value(row, 'treeid'),
                'matchid': get_value(row, 'matchid'),
                'surname': get_value(row, 'surname'),
                'given': get_value(row, 'given'),
                'birthdate': get_value(row, 'birthdate'),
                'deathdate': get_value(row, 'deathdate'),
                'birthplace': get_value(row, 'birthplace'),
                'deathplace': get_value(row, 'deathplace'),
                'sex': get_value(row, 'sex'),
                'relid': get_value(row, 'relid'),
                'personId': get_value(row, 'personId'),
                'fatherId': get_value(row, 'fatherId'),
                'motherId': get_value(row, 'motherId'),
                'spouseId': get_value(row, 'spouseId'),
                'source': get_value(row, 'source'),
                'created_date': get_value(row, 'created_date'),
                'birthdt1': get_value(row, 'birthdt1'),
                'birthdt2': get_value(row, 'birthdt2'),
                'deathdt1': get_value(row, 'deathdt1'),
                'deathdt2': get_value(row, 'deathdt2'),
            }
            DGIndividual.append(standardized_row)
        logging.info(f"FTDNA DGIndividual table standardized successfully: {DGIndividual[:5]}")

        # Process FTDNA DGTree table
        for row in ftdna_data.get('DGTree', []):
            standardized_row = {
                'Id': get_value(row, 'Id'),
                'name': get_value(row, 'name'),
                'treeid': get_value(row, 'treeid'),
                'treeurl': get_value(row, 'treeurl'),
                'basePersonId': get_value(row, 'basePersonId'),
                'CreateDate': get_value(row, 'CreateDate'),
                'UpdateDate': get_value(row, 'UpdateDate'),
                'matchID': get_value(row, 'matchID'),
                'source': get_value(row, 'source'),
            }
            DGTree.append(standardized_row)
        logging.info(f"FTDNA DGTree table standardized successfully: {DGTree[:5]}")

        # Process FTDNA DNA_Kits table
        for row in ftdna_data.get('DNA_Kits', []):
            if get_value(row, 'company') == 'FTDNA':
                standardized_row = {
                    'Id': get_value(row, 'Id'),
                    'company': get_value(row, 'company'),
                    'guid': get_value(row, 'guid'),
                    'name': get_value(row, 'name'),
                    'id2': get_value(row, 'id2'),
                    'id3': get_value(row, 'id3'),
                    'created_date': get_value(row, 'created_date'),
                }
                DNA_Kits.append(standardized_row)
        logging.info(f"FTDNA DNA_Kits table standardized successfully: {DNA_Kits[:5]}")

        # Process FTDNA_Matches2 table
        for row in ftdna_data['FTDNA_Matches2']:
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
                'Id': get_value(row, 'Id'),
                'DNAProvider': "2",
                'Surname': get_value(row, 'surname'),
                'Given': get_value(row, 'given_name'),
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
            FTDNA_Matches2.append(standardized_row)
        logging.info(f"FTDNA Matches2 table standardized successfully: {FTDNA_Matches2[:5]}")
    except KeyError as e:
        logging.error(f"Error standardizing FTDNA_data: Missing key {e}")
    return FTDNA_Matches2, DGIndividual, DGTree, DNA_Kits, FTDNA_ICW2, FTDNA_Matches2


def process_mh(mh_data):
    # Standardize Ancestry data to RootsMagic DB structure.
    DNA_Kits = []
    MH_ICW = []
    MH_Match = []
    MH_Tree = []

    def get_value(h_row, key, default=None):
        try:
            return h_row[key]
        except KeyError:
            return default

    try:
        logging.debug(f"Data Dict: DNA_Kits: {mh_data.get('DNA_Kits', [])[:5]}")
        logging.debug(f"Data Dict: MH_ICW : {mh_data.get('MH_ICW', [])[:5]}")
        logging.debug(f"Data Dict: MH_Match: {mh_data.get('MH_Match', [])[:5]}")
        logging.debug(f"Data Dict: MH_Tree: {mh_data.get('MH_Tree', [])[:5]}")

        # Process MH DNA_Kits
        for row in mh_data.get('DNA_Kits', []):
            if get_value(row, 'company') == 'MyHeritage':
                guid = get_value(row, 'guid')
                if guid.startswith('dnakit-'):
                    guid = guid[len('dnakit-'):]
                standardized_row = {
                    'Id': get_value(row, 'Id'),
                    'company': get_value(row, 'company'),
                    'guid': guid,
                    'name': get_value(row, 'name'),
                    'id2': get_value(row, 'id2'),
                    'id3': get_value(row, 'id3'),
                    'created_date': get_value(row, 'created_date'),
                }
                DNA_Kits.append(standardized_row)
        logging.info(f"MyHeritage DNA_Kits table standardized successfully: {DNA_Kits[:5]}")

        # Process MH_ICW table
        for row in mh_data['MH_ICW']:
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
                'Id': get_value(row, 'Id'),
                'DNAProvider': "2",
                'Surname': get_value(row, 'surname'),
                'Given': get_value(row, 'Given Name'),
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
            MH_ICW.append(standardized_row)
        logging.info(f"MyHeritage ICW data standardized successfully.{MH_ICW[:5]}")

        # Process MH_Match table
        for row in mh_data['MH_Match']:
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
                'Id': get_value(row, 'Id'),
                'DNAProvider': "2",
                'Surname': get_value(row, 'surname'),
                'Given': get_value(row, 'Given Name'),
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
            MH_Match.append(standardized_row)
        logging.info(f"MyHeritage Matches data standardized successfully.{MH_Match[:5]}")

        # Process MH_Tree table
        for row in mh_data['MH_Tree']:
            generated_uuid = uuid.uuid4()
            standardized_row = {
                'UniqueID': str(generated_uuid),
                'Id': get_value(row, 'Id'),
                'DNAProvider': "2",
                'Surname': get_value(row, 'surname'),
                'Given': get_value(row, 'Given Name'),
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
            MH_Tree.append(standardized_row)
        logging.info(f"MyHeritage Tree data standardized successfully.{MH_Tree[:5]}")
    except KeyError as e:
        logging.error(f"Error standardizing MyHeritage_data: Missing key {e}")
    return DNA_Kits, MH_ICW, MH_Match, MH_Tree


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
        logging.error(f"Error importing ancestry_data into RootsMagic database: {e}")
    finally:
        if cursor_rm:
            cursor_rm.close()
        # Do not close conn_rm here; it should be managed externally


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

            # Process user ancestry_data
            # process_user_kits(rootsmagic_conn, ancestry_data)

            # Process ancestry_data for each provider
            standardized_ancestry_data = process_ancestry(data['Ancestry'])
            standardized_ftdna_data = process_ftdna(data['FTDNA'])
            standardized_mh_data = process_mh(data['MyHeritage'])

            # Import standardized ancestry_data into RootsMagic
            # import_rm(rootsmagic_conn, standardized_ancestry_data)
            # import_rm(rootsmagic_conn, standardized_ftdna_data)
            # import_rm(rootsmagic_conn, standardized_mh_data)

            dnagedcom_conn.close()
        else:
            logging.error("Unable to establish database connection to DNAGedcom. Aborting.")
        rootsmagic_conn.close()
    else:
        logging.error("Unable to establish database connection to RootsMagic. Aborting.")
