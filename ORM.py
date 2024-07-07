import json
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from sqlalchemy import create_engine, Column, Integer, Float, Text, Index, BigInteger, event
from sqlalchemy.orm import sessionmaker, declarative_base

# Define global database paths. Switch the comment and uncomment on the lines below to use hard-coded database paths.
# DNAGEDCOM_DB_PATH = input("Enter the path to the DNAGedcom database: ")
# ROOTSMAGIC_DB_PATH = input("Enter the path to the RootsMagic database: ")
DNAGEDCOM_DB_PATH = r"Alerum68.db"
ROOTSMAGIC_DB_PATH = r"Alerum68 - Copy.rmtree"

Base = declarative_base()


class ChildTable(Base):
    # Define RootsMagic ChildTable
    __tablename__ = 'ChildTable'

    RecID = Column(Integer, primary_key=True)
    ChildID = Column(Integer)
    FamilyID = Column(Integer)
    RelFather = Column(Integer)
    RelMother = Column(Integer)
    ChildOrder = Column(Integer)
    IsPrivate = Column(Integer)
    ProofFather = Column(Integer)
    ProofMother = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for ChildTable
    idxChildOrder = Index('idxChildOrder', ChildTable.ChildOrder)
    idxChildID = Index('idxChildID', ChildTable.ChildID)
    idxChildFamilyID = Index('idxChildFamilyID', ChildTable.FamilyID)


class DNATable(Base):
    # Define RootsMagic DNATable
    __tablename__ = 'DNATable'

    RecID = Column(Integer, primary_key=True)
    ID1 = Column(Integer)
    ID2 = Column(Integer)
    Label1 = Column(Text)
    Label2 = Column(Text)
    DNAProvider = Column(Integer)
    SharedCM = Column(Float)
    SharedPercent = Column(Float)
    LargeSeg = Column(Float)
    SharedSegs = Column(Integer)
    Date = Column(Text)
    Relate1 = Column(Integer)
    Relate2 = Column(Integer)
    CommonAnc = Column(Integer)
    CommonAncType = Column(Integer)
    Verified = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for DNATable
    idxDnaId2 = Index('idxDnaId2', DNATable.ID2)
    idxDnaId1 = Index('idxDnaId1', DNATable.ID1)


class EventTable(Base):
    # Define RootsMagic EventTable
    __tablename__ = 'EventTable'
    EventID = Column(Integer, primary_key=True)
    EventType = Column(Integer)
    OwnerType = Column(Integer)
    OwnerID = Column(Integer)
    FamilyID = Column(Integer)
    PlaceID = Column(Integer)
    SiteID = Column(Integer)
    Date = Column(Text)
    SortDate = Column(BigInteger)
    IsPrimary = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Status = Column(Integer)
    Sentence = Column(Text)
    Details = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for EventTable
    idxOwnerEvent = Index('idxOwnerEvent', EventTable.OwnerID, EventTable.EventType)
    idxOwnerDate = Index('idxOwnerDate', EventTable.OwnerID, EventTable.SortDate)


class FactTypeTable(Base):
    # Define RootsMagic FactTypeTable
    __tablename__ = 'FactTypeTable'
    FactTypeID = Column(Integer, primary_key=True)
    OwnerType = Column(Integer)
    Name = Column(Text(collation='RMNOCASE'))
    Abbrev = Column(Text)
    GedcomTag = Column(Text)
    UseValue = Column(Integer)
    UseDate = Column(Integer)
    UsePlace = Column(Integer)
    Sentence = Column(Text)
    Flags = Column(Integer)
    UTCModDate = Column(Float)

    # Define indices for FactTypeTable
    idxFactTypeName = Index('idxFactTypeName', FactTypeTable.Name)
    idxFactTypeAbbrev = Index('idxFactTypeAbbrev', FactTypeTable.Abbrev)
    idxFactTypeGedcomTag = Index('idxFactTypeGedcomTag', FactTypeTable.GedcomTag)


class FamilyTable(Base):
    # Define RootsMagic FamilyTable
    __tablename__ = 'FamilyTable'
    FamilyID = Column(Integer, primary_key=True)
    FatherID = Column(Integer)
    MotherID = Column(Integer)
    ChildID = Column(Integer)
    HusbOrder = Column(Integer)
    WifeOrder = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    SpouseLabel = Column(Integer)
    FatherLabel = Column(Integer)
    MotherLabel = Column(Integer)
    SpouseLabelStr = Column(Text)
    FatherLabelStr = Column(Text)
    MotherLabelStr = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for FamilyTable
    idxFamilyMotherID = Index('idxFamilyMotherID', FamilyTable.MotherID)
    idxFamilyFatherID = Index('idxFamilyFatherID', FamilyTable.FatherID)


class GroupTable(Base):
    # Define RootsMagic GroupTable
    __tablename__ = 'GroupTable'
    RecID = Column(Integer, primary_key=True)
    GroupID = Column(Integer)
    StartID = Column(Integer)
    EndID = Column(Integer)
    UTCModDate = Column(Float)


class NameTable(Base):
    # Define RootsMagic NameTable
    __tablename__ = 'NameTable'
    NameID = Column(Integer, primary_key=True)
    OwnerID = Column(Integer)
    Surname = Column(Text(collation='RMNOCASE'))
    Given = Column(Text(collation='RMNOCASE'))
    Prefix = Column(Text(collation='RMNOCASE'))
    Suffix = Column(Text(collation='RMNOCASE'))
    Nickname = Column(Text(collation='RMNOCASE'))
    NameType = Column(Integer)
    Date = Column(Text)
    SortDate = Column(Integer)
    IsPrimary = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Sentence = Column(Text)
    Note = Column(Text)
    BirthYear = Column(Integer)
    DeathYear = Column(Integer)
    Display = Column(Integer)
    Language = Column(Text)
    UTCModDate = Column(Float)
    SurnameMP = Column(Text)
    GivenMP = Column(Text)
    NicknameMP = Column(Text)

    # Define indices for NameTable
    idxSurnameGiven = Index('idxSurnameGiven', NameTable.Surname, NameTable.Given, NameTable.BirthYear,
                            NameTable.DeathYear)
    idxSurnameGivenMP = Index('idxSurnameGivenMP', NameTable.SurnameMP, NameTable.GivenMP, NameTable.BirthYear,
                              NameTable.DeathYear)
    idxNamePrimary = Index('idxNamePrimary', NameTable.IsPrimary)
    idxGivenMP = Index('idxGivenMP', NameTable.GivenMP)
    idxNameOwnerID = Index('idxNameOwnerID', NameTable.OwnerID)
    idxGiven = Index('idxGiven', NameTable.Given)
    idxSurname = Index('idxSurname', NameTable.Surname)
    idxSurnameMP = Index('idxSurnameMP', NameTable.SurnameMP)


class PersonTable(Base):
    # Define RootsMagic PersonTable
    __tablename__ = 'PersonTable'
    PersonID = Column(Integer, primary_key=True)
    UniqueID = Column(Text)
    Sex = Column(Integer)
    ParentID = Column(Integer)
    SpouseID = Column(Integer)
    Color = Column(Integer)
    Color1 = Column(Integer)
    Color2 = Column(Integer)
    Color3 = Column(Integer)
    Color4 = Column(Integer)
    Color5 = Column(Integer)
    Color6 = Column(Integer)
    Color7 = Column(Integer)
    Color8 = Column(Integer)
    Color9 = Column(Integer)
    Relate1 = Column(Integer)
    Relate2 = Column(Integer)
    Flags = Column(Integer)
    Living = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Bookmark = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)


class PlaceTable(Base):
    # Define RootsMagic PlaceTable
    __tablename__ = 'PlaceTable'
    PlaceID = Column(Integer, primary_key=True)
    PlaceType = Column(Integer)
    Name = Column(Text(collation='RMNOCASE'))
    Abbrev = Column(Text)
    Normalized = Column(Text)
    Latitude = Column(Integer)
    Longitude = Column(Integer)
    LatLongExact = Column(Integer)
    MasterID = Column(Integer)
    Note = Column(Text)
    Reverse = Column(Text(collation='RMNOCASE'))
    fsID = Column(Integer)
    anID = Column(Integer)
    UTCModDate = Column(Float)

    # Define indices for PlaceTable
    idxPlaceName = Index('idxPlaceName', PlaceTable.Name)
    idxPlaceAbbrev = Index('idxPlaceAbbrev', PlaceTable.Abbrev)
    idxReversePlaceName = Index('idxReversePlaceName', PlaceTable.Reverse)


class URLTable(Base):
    # Define RootsMagic PlaceTable
    __tablename__ = 'URLTable'
    LinkID = Column(Integer, primary_key=True)
    OwnerType = Column(Integer)
    OwnerID = Column(Integer)
    LinkType = Column(Integer)
    Name = Column(Text)
    URL = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)


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
        file_handler_info = RotatingFileHandler('DG2RM.log', maxBytes=1000000, backupCount=3, encoding='utf-8')
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
    # Connect to databases, and create SQLAlchemy engines and sessions.
    try:
        engine = create_engine(f"sqlite:///{db_path}", echo=False)

        if db_name == "RootsMagic":
            @event.listens_for(engine, "connect")
            def set_sqlite_pragma(dbapi_connection):
                # This function sets SQLite PRAGMA statements upon connection
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA case_sensitive_like=ON")
                cursor.close()

        logging.info(f"Connected to {db_name or 'database'} database at: {db_path}")

        Session = sessionmaker(bind=engine)
        session = Session()

        return session, engine

    except Exception as e:
        logging.error(f"Error connecting to {db_name or 'database'} database: {e}")
        return None, None


def load_ethnicity_mappings():
    # Load ethnicity mappings from a JSON file
    script_dir = os.path.dirname(__file__)
    json_path = os.path.join(script_dir, "ethnicities.json")
    with open(json_path, 'r') as f:
        return json.load(f)


def dna_kit_fact_type(session_rm):
    # Insert or update the "DNA Kit" fact type in RootsMagic database
    try:
        fact_type = session_rm.query(FactTypeTable).filter_by(Name='DNA Kit').first()
        if not fact_type:
            new_fact_type = FactTypeTable(
                OwnerType=0,
                Name='DNA Kit',
                UseValue=1,
                Abbrev='DNA Kit',
                GedcomTag='EVEN',
                Sentence='[person] had a DNA test performed. Kit #:< [Desc]>.'
            )
            session_rm.add(new_fact_type)
            session_rm.commit()
            logging.info("DNA Kit fact type added successfully")
        else:
            logging.info("'DNA Kit' already exists in FactTypeTable. Skipped.")
    except Exception as e:
        logging.error(f"Error adding DNA Kit fact type: {e}")


def gather_data(session_dg, size=None):
    # Assigning what tables to insert into the data Dictionary
    local_data = {}

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
                query = session_dg.query(table)
                if size:
                    query = query.limit(size)
                rows = query.all()
                local_data[provider][table] = [row.__dict__ for row in rows]

    except Exception as e:
        logging.error(f"Error gathering ancestry_data: {e}")

    return local_data


def rebuild_indices(engine):
    metadata = MetaData(bind=engine)

    # List of tables that require index rebuilding
    tables_to_rebuild_indices = [
        ChildTable,
        DNATable,
        EventTable,
        FactTypeTable,
        FamilyTable,
        NameTable,
        PlaceTable
    ]

    # Drop and recreate indices for each table
    for table in tables_to_rebuild_indices:
        table_name = table.__table__.name  # Get the table name
        table_metadata = metadata.tables[table_name]

        # Iterate through indices defined in the table's metadata
        for index in table_metadata.indexes:
            index.drop(engine, checkfirst=True)  # Drop existing index
            index.create(engine, checkfirst=True)  # Recreate index


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
                'MotherAmtGid': get_value(row, 'MotherAmtGid'),
                'MotherBigTreeGid': get_value(row, 'MotherBigTreeGid'),
                'MotherKinshipPathToSampleId': get_value(row, 'MotherKinshipPathToSampleId'),
                'MotherKinshipPathFromSampleToMatch': get_value(row, 'MotherKinshipPathFromSampleToMatch'),
                'MotherPotential': get_value(row, 'MotherPotential'),
                'MotherInMatchTree': get_value(row, 'MotherInMatchTree'),
                'MotherInBestContributorTree': get_value(row, 'MotherInBestContributorTree'),
                'TriangulationGroupId': get_value(row, 'TriangulationGroupId')
            }
            AncestryAncestorCouple.append(standardized_row)

        # Process Ancestry_Ethnicity table
        for row in ancestry_data.get('Ancestry_Ethnicity', []):
            standardized_row = {
                'ID1': get_value(row, 'TestGuid'),
                'ID2': get_value(row, 'MatchGuid'),
                'Ethnicity': get_value(row, 'Ethnicity'),
                'Confidence': get_value(row, 'Confidence')
            }
            Ancestry_Ethnicity.append(standardized_row)

        # Process Ancestry_ICW table
        for row in ancestry_data.get('Ancestry_ICW', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'ICWGuid': get_value(row, 'ICWGuid')
            }
            Ancestry_icw.append(standardized_row)

        # Process Ancestry_matchEthnicity table
        for row in ancestry_data.get('Ancestry_matchEthnicity', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'Ethnicity': get_value(row, 'Ethnicity'),
                'Confidence': get_value(row, 'Confidence')
            }
            Ancestry_matchEthnicity.append(standardized_row)

        # Process Ancestry_matchGroups table
        for row in ancestry_data.get('Ancestry_matchGroups', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'GroupName': get_value(row, 'GroupName'),
                'Confidence': get_value(row, 'Confidence')
            }
            Ancestry_matchGroups.append(standardized_row)

        # Process Ancestry_matchTrees table
        for row in ancestry_data.get('Ancestry_matchTrees', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'TreeGuid': get_value(row, 'TreeGuid'),
                'TreeUrl': get_value(row, 'TreeUrl')
            }
            Ancestry_matchTrees.append(standardized_row)

        # Process Ancestry_profiles table
        for row in ancestry_data.get('Ancestry_Profiles', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'Name': get_value(row, 'Name'),
                'Gender': get_value(row, 'Gender'),
                'BirthYear': get_value(row, 'BirthYear'),
                'DeathYear': get_value(row, 'DeathYear')
            }
            Ancestry_profiles.append(standardized_row)

        # Process Ancestry_TreeData table
        for row in ancestry_data.get('Ancestry_TreeData', []):
            standardized_row = {
                'TreeGuid': get_value(row, 'TreeGuid'),
                'PersonGuid': get_value(row, 'PersonGuid'),
                'Name': get_value(row, 'Name'),
                'Gender': get_value(row, 'Gender'),
                'BirthYear': get_value(row, 'BirthYear'),
                'DeathYear': get_value(row, 'DeathYear'),
                'Relationship': get_value(row, 'Relationship')
            }
            Ancestry_TreeData.append(standardized_row)

        standardized_data = {
            'AncestryAncestorCouple': AncestryAncestorCouple,
            'Ancestry_Ethnicity': Ancestry_Ethnicity,
            'Ancestry_icw': Ancestry_icw,
            'Ancestry_matchEthnicity': Ancestry_matchEthnicity,
            'Ancestry_matchGroups': Ancestry_matchGroups,
            'Ancestry_matchTrees': Ancestry_matchTrees,
            'Ancestry_profiles': Ancestry_profiles,
            'Ancestry_TreeData': Ancestry_TreeData
        }
        logging.info("Ancestry data processed and standardized")
        return standardized_data

    except Exception as e:
        logging.error(f"Error processing ancestry_data: {e}")


def process_ftdna(ftdna_data):
    # Standardize FTDNA data to RootsMagic DB structure.
    FTDNA_matches = []
    FTDNA_icw = []
    FTDNA_Trees = []
    FTDNA_profiles = []

    def get_value(a_row, key, default=None):
        try:
            return a_row[key]
        except KeyError:
            return default

    try:
        logging.debug(f"Data Dict: FTDNA_matches: {ftdna_data.get('FTDNA_matches', [])[:5]}")
        logging.debug(f"Data Dict: FTDNA_icw: {ftdna_data.get('FTDNA_icw', [])[:5]}")
        logging.debug(f"Data Dict: FTDNA_Trees: {ftdna_data.get('FTDNA_Trees', [])[:5]}")
        logging.debug(f"Data Dict: FTDNA_profiles: {ftdna_data.get('FTDNA_profiles', [])[:5]}")

        # Process FTDNA_matches table
        for row in ftdna_data.get('FTDNA_matches', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'MatchName': get_value(row, 'MatchName'),
                'SharedCM': get_value(row, 'SharedCM'),
                'LongestBlock': get_value(row, 'LongestBlock'),
                'Xmatch': get_value(row, 'Xmatch'),
                'RelationshipRange': get_value(row, 'RelationshipRange'),
                'Email': get_value(row, 'Email')
            }
            FTDNA_matches.append(standardized_row)

        # Process FTDNA_icw table
        for row in ftdna_data.get('FTDNA_icw', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'ICWGuid': get_value(row, 'ICWGuid')
            }
            FTDNA_icw.append(standardized_row)

        # Process FTDNA_Trees table
        for row in ftdna_data.get('FTDNA_Trees', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'MatchGuid': get_value(row, 'MatchGuid'),
                'TreeGuid': get_value(row, 'TreeGuid'),
                'TreeUrl': get_value(row, 'TreeUrl')
            }
            FTDNA_Trees.append(standardized_row)

        # Process FTDNA_profiles table
        for row in ftdna_data.get('FTDNA_profiles', []):
            standardized_row = {
                'TestGuid': get_value(row, 'TestGuid'),
                'Name': get_value(row, 'Name'),
                'Gender': get_value(row, 'Gender'),
                'BirthYear': get_value(row, 'BirthYear'),
                'DeathYear': get_value(row, 'DeathYear')
            }
            FTDNA_profiles.append(standardized_row)

        standardized_data = {
            'FTDNA_matches': FTDNA_matches,
            'FTDNA_icw': FTDNA_icw,
            'FTDNA_Trees': FTDNA_Trees,
            'FTDNA_profiles': FTDNA_profiles
        }
        logging.info("FTDNA data processed and standardized")
        return standardized_data

    except Exception as e:
        logging.error(f"Error processing ftdna_data: {e}")


def process_mh(mh_data):
    # Standardize MyHeritage data to RootsMagic DB structure.
    MH_matches = []
    MH_icw = []
    MH_Trees = []
    MH_profiles = []

    def get_value(a_row, key, default=None):
        try:
            return a_row[key]
        except KeyError:
            return default

    try:
        logging.debug(f"Data Dict: MH_matches: {mh_data.get('MH_matches', [])[:5]}")
        logging.debug(f"Data Dict: MH_icw: {mh_data.get('MH_icw', [])[:5]}")
        logging.debug(f"Data Dict: MH_Trees: {mh_data.get('MH_Trees', [])[:5]}")
        logging.debug(f"Data Dict: MH_profiles: {mh_data.get('MH_profiles', [])[:5]}")

        # Process MH_matches table
        for row in mh_data.get('MH_matches', []):
            standardized_row = {
                'MatchID': get_value(row, 'MatchID'),
                'MatchName': get_value(row, 'MatchName'),
                'SharedCM': get_value(row, 'SharedCM'),
                'SharedSegments': get_value(row, 'SharedSegments'),
                'LargestSegment': get_value(row, 'LargestSegment'),
                'EstimatedRelationship': get_value(row, 'EstimatedRelationship'),
                'Email': get_value(row, 'Email')
            }
            MH_matches.append(standardized_row)

        # Process MH_icw table
        for row in mh_data.get('MH_icw', []):
            standardized_row = {
                'MatchID': get_value(row, 'MatchID'),
                'ICWMatchID': get_value(row, 'ICWMatchID')
            }
            MH_icw.append(standardized_row)

        # Process MH_Trees table
        for row in mh_data.get('MH_Trees', []):
            standardized_row = {
                'MatchID': get_value(row, 'MatchID'),
                'TreeID': get_value(row, 'TreeID'),
                'TreeURL': get_value(row, 'TreeURL')
            }
            MH_Trees.append(standardized_row)

        # Process MH_profiles table
        for row in mh_data.get('MH_profiles', []):
            standardized_row = {
                'ProfileID': get_value(row, 'ProfileID'),
                'Name': get_value(row, 'Name'),
                'Gender': get_value(row, 'Gender'),
                'BirthYear': get_value(row, 'BirthYear'),
                'DeathYear': get_value(row, 'DeathYear')
            }
            MH_profiles.append(standardized_row)

        standardized_data = {
            'MH_matches': MH_matches,
            'MH_icw': MH_icw,
            'MH_Trees': MH_Trees,
            'MH_profiles': MH_profiles
        }
        logging.info("MyHeritage data processed and standardized")
        return standardized_data

    except Exception as e:
        logging.error(f"Error processing mh_data: {e}")


def import_rm(session_rm, standardized_ancestry_data, standardized_ftdna_data, standardized_mh_data):
    try:
        # Import Ancestry Data
        for row in standardized_ancestry_data['AncestryAncestorCouple']:
            session_rm.add(AncestryAncestorCouple(**row))
        for row in standardized_ancestry_data['Ancestry_Ethnicity']:
            session_rm.add(Ancestry_Ethnicity(**row))
        # Similarly, add for other tables...

        # Import FTDNA Data
        for row in standardized_ftdna_data['FTDNA_Matches2']:
            session_rm.add(FTDNA_Matches2(**row))
        # Similarly, add for other tables...

        # Import MyHeritage Data
        for row in standardized_mh_data['MH_Match']:
            session_rm.add(MH_Match(**row))
        # Similarly, add for other tables...

        session_rm.commit()
        logging.info("Data imported into RootsMagic database successfully")
    except Exception as e:
        session_rm.rollback()
        logging.error(f"Error importing data into RootsMagic database: {e}")


def main():
    setup_logging()
    session_dg, engine_dg = connect_to_db(DNAGEDCOM_DB_PATH)
    session_rm, engine_rm = connect_to_db(ROOTSMAGIC_DB_PATH, "RootsMagic")

    if session_dg and session_rm:
        dna_kit_fact_type(session_rm)
        data = gather_data(session_dg, size=None)
        ancestry_data = process_ancestry(data.get('Ancestry', {}))
        ftdna_data = process_ftdna(data.get('FTDNA', {}))
        mh_data = process_mh(data.get('MyHeritage', {}))

        # import_rm(session_rm, ancestry_data, ftdna_data, mh_data)
        # rebuild_indices(rootsmagic_engine)

        engine = create_engine(f'sqlite:///{ROOTSMAGIC_DB_PATH}')
        create_indices(engine)

        session_dg.close()
        session_rm.close()
    else:
        logging.error("Failed to create database sessions")


if __name__ == "__main__":
    main()
