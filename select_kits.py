from sqlalchemy import Column, Integer, String, Float, Index, UniqueConstraint,  BigInteger, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import re
import logging

Base = declarative_base()

class Ancestry_matchGroups(Base):
    __tablename__ = 'Ancestry_matchGroups'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    testGuid = Column(String)
    matchGuid = Column(String)
    matchTestDisplayName = Column(String)
    matchTestAdminDisplayName = Column(String)
    matchTreeNodeCount = Column(Integer)
    groupName = Column(String)
    confidence = Column(Float)
    sharedCentimorgans = Column(Float)
    fetchedSegmentInfo = Column(Integer)
    totalSharedCentimorgans = Column(Float)
    longestSegmentCentimorgans = Column(Float)
    sharedSegment = Column(Integer)
    lastLoggedInDate = Column(String)
    starred = Column(String)
    viewed = Column(String)
    matchTreeIsPrivate = Column(String)
    hasHint = Column(String)
    note = Column(String)
    userPhoto = Column(String)
    matchTreeId = Column(String)
    treeId = Column(String)
    matchMemberSinceYear = Column(String)
    created_date = Column(String)
    icwRunDate = Column(String)
    treeRunDate = Column(String)
    matchRunDate = Column(String)
    loginUsername = Column(String)
    sync = Column(String)
    matchTestAdminUcdmId = Column(String)
    GF_Sync = Column(String)
    paternal = Column(Integer)
    maternal = Column(Integer)
    subjectGender = Column(String)
    meiosisValue = Column(Integer)
    parentCluster = Column(String)

    __table_args__ = (
        UniqueConstraint('matchGuid', 'testGuid', 'sharedCentimorgans', 'groupName', name='IDX_Ancestry_matchGroups5'),
        UniqueConstraint('testGuid', 'matchGuid', 'groupName', name='IDX_Ancestry_matchGroups3'),
        UniqueConstraint('testGuid', 'matchGuid', name='IDX_Ancestry_matchGroups'),
        UniqueConstraint('testGuid', 'matchGuid', 'sharedCentimorgans', name='IDX_Ancestry_matchGroups4'),
        Index('IDX_Ancestry_matchGroups2', 'matchGuid'),
    )

class FTDNA_Matches2(Base):
    __tablename__ = 'FTDNA_Matches2'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    eKit1 = Column(String)
    eKit2 = Column(String)
    Name = Column(String)
    MatchPersonName = Column(String)
    Prefix = Column(String)
    FirstName = Column(String)
    MiddleName = Column(String)
    LastName = Column(String)
    Suffix = Column(String)
    Email = Column(String)
    HasFamilyTree = Column(String)
    ThirdParty = Column(String)
    Note = Column(String)
    Female = Column(String)
    contactId = Column(String)
    AboutMe = Column(String)
    PaternalAncestorName = Column(String)
    MaternalAncestorName = Column(String)
    strRbdate = Column(String)
    Relationship = Column(String)
    strRelationshipRange = Column(String)
    strSuggestedRelationship = Column(String)
    strRelationshipName = Column(String)
    totalCM = Column(String)
    userSurnames = Column(String)
    longestCentimorgans = Column(String)
    ydnaMarkers = Column(String)
    mtDNAMarkers = Column(String)
    yHaplo = Column(String)
    mtHaplo = Column(String)
    isxMatch = Column(String)
    ffBack = Column(String)
    bucketType = Column(String)
    nRownum = Column(String)
    familyTreeUrl = Column(String)
    created_date = Column(String)
    icw_date = Column(BigInteger)
    icw_tree = Column(BigInteger)
    totalCMf = Column(Float)
    longestCentimorgansf = Column(Float)
    GF_Sync = Column(String)

class MH_Match(Base):
    __tablename__ = 'MH_Match'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    kitId = Column(Integer)
    guid = Column(String, unique=True)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    age = Column(String)
    birth_place = Column(String)
    gender = Column(String)
    country = Column(String)
    creatorid = Column(String)
    contact = Column(String)
    manager = Column(String)
    contact_manager = Column(String)
    status = Column(String)
    estimated_relationship = Column(String)
    totalCM = Column(Float)
    percent_shared = Column(Float)
    num_segments = Column(Integer)
    largestCM = Column(Float)
    has_tree = Column(Integer)
    tree_size = Column(Integer)
    tree_url = Column(String)
    person_url = Column(String)
    smart_matches = Column(Integer)
    shared_surnames = Column(String)
    surnames = Column(String)
    notes = Column(String)
    CreatedDate = Column(String)
    icwRunDate = Column(String)
    icwcMlow = Column(Float)
    triagRunDate = Column(String)
    treeRunDate = Column(String)
    chromoRunDate = Column(String)
    gmUploadDate = Column(String)
    testid = Column(String)
    matchid = Column(String)
    GF_Sync = Column(String)

    __table_args__ = (
        Index('IDX_MH_MATCH', 'guid'),
    )


# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Define ORM base and classes
Base = declarative_base()

class AncestryProfile(Base):
    __tablename__ = 'Ancestry_Profiles'
    guid = Column(String, primary_key=True)
    name = Column(String)

class DNAKit(Base):
    __tablename__ = 'DNA_Kits'
    company = Column(String)
    guid = Column(String, primary_key=True)
    name = Column(String)
    

# Initialize database connection
def init_db(database_url):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

# Fetch user kit data
def user_kit_data(session):
    dna_kits = []
    try:
        # Query Ancestry_Profiles
        ancestry_profiles = session.query(AncestryProfile).all()
        if not ancestry_profiles:
            logger.debug("No data found in Ancestry_Profiles.")
        else:
            logger.debug(f"Found {len(ancestry_profiles)} entries in Ancestry_Profiles.")
        
        dna_kits.extend(
            [(2, profile.guid, *(profile.name.rsplit(' ', 1) if ' ' in profile.name else (profile.name, ''))) 
             for profile in ancestry_profiles]
        )
        
        # Query DNA_Kits for FTDNA and MyHeritage
        dna_kits_query = session.query(DNAKit).filter(DNAKit.company.in_(['FTDNA', 'MyHeritage'])).all()
        if not dna_kits_query:
            logger.debug("No data found in DNA_Kits for FTDNA or MyHeritage.")
        else:
            logger.debug(f"Found {len(dna_kits_query)} entries in DNA_Kits for FTDNA or MyHeritage.")
        
        for kit in dna_kits_query:
            guid = kit.guid
            name = kit.name
            if kit.company == 'MyHeritage':
                guid = guid.replace('dnakit-', '')
                name = re.sub(r'\(.*\)', '', name).strip()
            dna_kits.append(
                (5 if kit.company == 'MyHeritage' else 3, guid, *(name.rsplit(' ', 1) if ' ' in name else (name, '')))
            )
        logging.info("Extracted user kit ancestry_data successfully.")
    except Exception as e:
        logging.error(f"Error extracting user kit ancestry_data: {e}")
    return dna_kits

# Prompt user for kits
def prompt_user_for_kits(dna_kits):
    print("Available Kits:")
    for idx, kit in enumerate(dna_kits):
        print(f"{idx + 1}: {kit[1]} (Name: {kit[2]} {kit[3]})")
    
    selected_indices = input("Enter the numbers of the kits you want to run the script on (comma-separated): ")
    selected_indices = [int(i) - 1 for i in selected_indices.split(',')]
    selected_kits = [dna_kits[i] for i in selected_indices]
    
    return selected_kits

# Process selected kits
def process_kit(session, kit):
    # Define the logic to process each kit
    logger.info(f"Processing kit: {kit[1]} (Name: {kit[2]} {kit[3]})")
    
    # Example: Filtering and processing data for the selected kit
    guid = kit[1]
    
    # Fetch related data for the selected kit from relevant tables
    related_profiles = session.query(AncestryProfile).filter(AncestryProfile.guid == guid).all()
    related_match_ethnicity = session.query(AncestryMatchEthnicity).filter(AncestryMatchEthnicity.match_id == guid).all()
    related_match_groups = session.query(AncestryMatchGroups).filter(AncestryMatchGroups.match_id == guid).all()
    
    # Process fetched data (example logic, modify as needed)
    for profile in related_profiles:
        logger.info(f"Profile: {profile.guid} - {profile.name}")
    
    for match_ethnicity in related_match_ethnicity:
        logger.info(f"Match Ethnicity: {match_ethnicity.match_id} - {match_ethnicity.ethnicity}")
    
    for match_group in related_match_groups:
        logger.info(f"Match Group: {match_group.match_id} - {match_group.group_name}")

# Main function
def main(database_url):
    Session = init_db(database_url)
    session = Session()
    
    # Fetch user kit data
    dna_kits = user_kit_data(session)
    if not dna_kits:
        logger.error("No DNA kits found in the database.")
        return
    
    # Prompt user for kits
    selected_kits = prompt_user_for_kits(dna_kits)
    logger.info(f"Selected kits: {selected_kits}")
    
    # Process selected kits
    for kit in selected_kits:
        process_kit(session, kit)

if __name__ == "__main__":
    database_url = "sqlite:///Alerum68.db"  # Update with your actual database URL
    main(database_url)
