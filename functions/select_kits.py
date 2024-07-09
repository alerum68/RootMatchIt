import logging
import re
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base
from database import connect_to_db_sqlalchemy, DNAGEDCOM_DB_PATH
from setup_logging import setup_logging

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


def user_kit_data(session):
    dna_kits = []
    try:
        # Query Ancestry_Profiles
        ancestry_profiles = session.query(AncestryProfile).all()
        logging.debug(f"Found {len(ancestry_profiles)} entries in Ancestry_Profiles.")

        dna_kits.extend(
            [(2, profile.guid, *(profile.name.rsplit(' ', 1) if ' ' in profile.name else (profile.name, '')))
             for profile in ancestry_profiles]
        )

        # Query DNA_Kits for FTDNA and MyHeritage
        dna_kits_query = session.query(DNAKit).filter(DNAKit.company.in_(['FTDNA', 'MyHeritage'])).all()
        logging.debug(f"Found {len(dna_kits_query)} entries in DNA_Kits for FTDNA or MyHeritage.")

        for kit in dna_kits_query:
            guid = kit.guid
            name = kit.name
            if kit.company == 'MyHeritage':
                guid = guid.replace('dnakit-', '')
                name = re.sub(r'\(.*\)', '', name).strip()
            dna_kits.append(
                (5 if kit.company == 'MyHeritage' else 3, guid, *(name.rsplit(' ', 1) if ' ' in name else (name, '')))
            )
        logging.info("Extracted user kits successfully.")
    except Exception as e:
        logging.error(f"Error extracting user kit data: {e}")
    return dna_kits


def prompt_user_for_kits(dna_kits):
    # Ask what kits to select.
    ancestry_kits = [kit for kit in dna_kits if kit[0] == 2]
    ftdna_kits = [kit for kit in dna_kits if kit[0] == 3]
    myheritage_kits = [kit for kit in dna_kits if kit[0] == 5]

    print("Ancestry Kits:")
    for idx, kit in enumerate(ancestry_kits, 1):
        print(f"{idx}: {kit[1]} (Name: {kit[2]} {kit[3]})")

    print("\nFamily Tree DNA (FTDNA) Kits:")
    for idx, kit in enumerate(ftdna_kits, len(ancestry_kits) + 1):
        print(f"{idx}: {kit[1]} (Name: {kit[2]} {kit[3]})")

    print("\nMyHeritage Kits :")
    for idx, kit in enumerate(myheritage_kits, len(ancestry_kits) + len(ftdna_kits) + 1):
        print(f"{idx}: {kit[1]} (Name: {kit[2]} {kit[3]})")

    selected_indices = input(
        "Enter the numbers of the kits you want to run the script on (comma-separated), "
        "or enter company name (case-insensitive) or first letter, or press Enter for all: "
    )
    selected_kits = []

    if selected_indices.strip() == "":
        selected_kits = dna_kits  # Select all kits
    else:
        selected_indices = [idx.strip() for idx in selected_indices.split(',')]
        for idx in selected_indices:
            # Check if it's a number
            if idx.isdigit():
                idx = int(idx) - 1
                if 0 <= idx < len(dna_kits):
                    selected_kits.append(dna_kits[idx])
            else:
                # Check if it's a company name or first letter
                company_map = {'a': 2, 'ancestry': 2, 'f': 3, 'ftdna': 3, 'm': 5, 'myheritage': 5}
                company_id = company_map.get(idx.lower())
                if company_id:
                    selected_kits.extend([kit for kit in dna_kits if kit[0] == company_id])

    return selected_kits


def main():
    logging.info("Connecting to database...")
    session, _ = connect_to_db_sqlalchemy(DNAGEDCOM_DB_PATH)

    logging.info("Fetching user kit data...")
    dna_kits = user_kit_data(session)

    if dna_kits:
        logging.info("Prompting user for kits...")
        selected_kits = prompt_user_for_kits(dna_kits)

        logging.info("Selected kits:")
        for kit in selected_kits:
            logging.info(f"Type: {kit[0]}, GUID: {kit[1]}, Name: {kit[2]} {kit[3]}")
    else:
        logging.warning("No kits found.")


if __name__ == "__main__":
    setup_logging()
    main()
