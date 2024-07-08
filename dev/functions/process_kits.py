import re
import logging
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import declarative_base, sessionmaker

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

# Add other necessary ORM classes based on the provided information

# Example placeholder classes for other relevant tables (expand as needed)
class AncestryMatchEthnicity(Base):
    __tablename__ = 'Ancestry_matchEthnicity'
    id = Column(Integer, primary_key=True)
    match_id = Column(String)
    ethnicity = Column(String)

class AncestryMatchGroups(Base):
    __tablename__ = 'Ancestry_matchGroups'
    id = Column(Integer, primary_key=True)
    match_id = Column(String)
    group_name = Column(String)

class AncestryTrees(Base):
    __tablename__ = 'Ancestry_Trees'
    id = Column(Integer, primary_key=True)
    tree_id = Column(String)
    tree_name = Column(String)

class AncestryAncestorCouple(Base):
    __tablename__ = 'AncestryAncestorCouple'
    id = Column(Integer, primary_key=True)
    ancestor_id = Column(String)
    couple_name = Column(String)

class AncestryTreeData(Base):
    __tablename__ = 'Ancestry_TreeData'
    id = Column(Integer, primary_key=True)
    tree_data_id = Column(String)
    data_value = Column(String)

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
    # Example: You can add more detailed processing logic here
    # ...

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
  
