# import_data.py
# Local Imports
from a_classes import *
from database import connect_to_db_sqlalchemy, find_database_paths
from ftdna_classes import *
from mh_classes import *
from rm_classes import *
from select_kits import user_kit_data, prompt_user_for_kits
from setup_logging import setup_logging
# Remote Imports
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import uuid

ancestry_base = Ancestry_Base()
ftdna_base = FTDNA_Base()
mh_base = MH_Base()
rm_base = RM_Base()

# Switches
limit = 50
ancestry_matchgroups = 1
ancestry_matchtrees = 0
ftdna_matches2 = 0
mh_match = 0


def process_table_with_limit(session, table_class, filter_ids, process_func, apply_limit):
    query = session.query(table_class).filter(table_class.Id.in_(filter_ids))
    if apply_limit > 0:
        query = query.limit(apply_limit)
    limited_data = query.all()
    processed_data = [process_func(item) for item in limited_data]
    return processed_data[:apply_limit] if apply_limit > 0 else processed_data


def generate_unique_id(*args) -> str:
    filtered_args = [str(arg) for arg in args if arg]
    if not filtered_args:
        return str(uuid.uuid4())
    unique_str = ' '.join(filtered_args)
    namespace = uuid.NAMESPACE_DNS
    unique_id = str(uuid.uuid5(namespace, unique_str))
    return unique_id


def check_for_duplicates(session: Session, unique_id: str, **kwargs):
    logger = logging.getLogger('get_or_create_person')
    try:
        person = session.query(PersonTable).filter_by(UniqueID=unique_id).first()
        if person:
            for key, value in kwargs.items():
                setattr(person, key, value)
            logger.info(f"Updated existing person: {unique_id}")
        else:
            person = PersonTable(UniqueID=unique_id, **kwargs)
            session.add(person)
            logger.info(f"Created new person: {unique_id}")
        return person
    except SQLAlchemyError as dup_e:
        logger.error(f"Error in get_or_create_person for {unique_id}: {dup_e}")
        session.rollback()
        return None


def filter_selected_kits(filter_session: Session, f_selected_kits):
    global ancestry_matchgroups, ancestry_matchtrees, ftdna_matches2, mh_match
    logger = logging.getLogger('filter_selected_kits')
    logger.info("Filtering selected kits...")

    selected_guids = [kit[1] for kit in f_selected_kits]
    test_ids = {'Ancestry_matchGroups': [], 'Ancestry_matchTrees': [], 'FTDNA_Matches2': [], 'MH_Match': []}

    try:
        if ancestry_matchgroups:
            ancestry_matches = filter_session.query(Ancestry_matchGroups).filter(
                Ancestry_matchGroups.testGuid.in_(selected_guids)).all()
            test_ids['Ancestry_matchGroups'] = [match.Id for match in ancestry_matches]

            # Get matchGuids for use in ancestry_matchtrees
            match_guids = []

            if ancestry_matches:
                match_guids = [group.matchGuid for group in ancestry_matches]

            if ancestry_matchtrees:
                # Use both selected_guids and match_guids (if available) for ancestry_matchtrees
                guids_to_check = selected_guids + match_guids
                ancestry_matches_trees = filter_session.query(Ancestry_matchTrees).filter(
                    Ancestry_matchTrees.matchid.in_(guids_to_check)).all()
                test_ids['Ancestry_matchTrees'] = [match.Id for match in ancestry_matches_trees]

    except Exception as filter_e:
        logger.error(f"Error filtering selected kits: {filter_e}")
        raise

    return test_ids


def process_ancestry(session: Session, filtered_ids):
    global limit
    logger = logging.getLogger('process_ancestry')
    logger.info("Processing Ancestry data...")

    processed_ancestry_data = []

    try:
        if ancestry_matchgroups and filtered_ids.get('Ancestry_matchGroups'):
            def process_matchgroup(group):
                return {
                    'unique_id': generate_unique_id(group.testGuid, group.matchGuid),
                    'sex': 1 if group.subjectGender == 'F' else 0 if group.subjectGender == 'M' else 2,
                    'color': 25
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_matchGroups, filtered_ids['Ancestry_matchGroups'],
                process_matchgroup, limit
            ))

        if ancestry_matchtrees and filtered_ids.get('Ancestry_matchTrees'):
            def process_matchtree(individual):
                sex = 2  # Default to Unknown
                if individual.personId:
                    if individual.fatherId and individual.personId in individual.fatherId:
                        sex = 0  # Male
                    elif individual.motherId and individual.personId in individual.motherId:
                        sex = 1  # Female

                return {
                    'unique_id': generate_unique_id(individual.given, individual.surname, individual.birthdate),
                    'sex': sex,
                    'color': 24
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_matchTrees, filtered_ids['Ancestry_matchTrees'],
                process_matchtree, limit
            ))

    except Exception as e:
        logger.error(f"Error processing Ancestry data: {e}")

    return processed_ancestry_data


def process_ftdna(session: Session, filtered_ids):
    global limit
    logger = logging.getLogger('process_ftdna')
    logger.info("Processing FTDNA data...")

    processed_ftdna_data = []

    try:
        if ftdna_matches2 and filtered_ids.get('FTDNA_Matches2'):
            def process_ftdna_match(match):
                return {
                    'unique_id': generate_unique_id(match.eKit1, match.eKit2),
                    'sex': 1 if match.Female else 0,
                    'color': 26
                }

            processed_ftdna_data.extend(process_table_with_limit(
                session, FTDNA_Matches2, filtered_ids['FTDNA_Matches2'],
                process_ftdna_match, limit
            ))

    except Exception as e:
        logger.error(f"Error processing FTDNA data: {e}")

    return processed_ftdna_data


def process_mh(session: Session, filtered_ids):
    global limit
    logger = logging.getLogger('process_mh')
    logger.info("Processing MyHeritage data...")

    processed_mh_data = []

    try:
        if mh_match and filtered_ids.get('MH_Match'):
            def process_mh_match(match):
                return {
                    'unique_id': generate_unique_id(match.guid),
                    'sex': 1 if match.gender == 'F' else 0 if match.gender == 'M' else 2,
                    'color': 27
                }

            processed_mh_data.extend(process_table_with_limit(
                session, MH_Match, filtered_ids['MH_Match'],
                process_mh_match, limit
            ))

    except Exception as e:
        logger.error(f"Error processing MyHeritage data: {e}")

    return processed_mh_data


def insert_person(person_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_person')
    logger.info("Inserting or updating individuals in PersonTable...")

    try:
        processed_count = 0

        for data in processed_data:
            check_for_duplicates(person_rm_session,
                                 data['unique_id'],
                                 Sex=data['sex'],
                                 Color=data['color'],
                                 )
            processed_count += 1
            if processed_count % batch_size == 0:
                person_rm_session.flush()

        person_rm_session.commit()
        logger.info(f"Processed {processed_count} individuals.")
    except Exception as e:
        logger.error(f"Error inserting or updating PersonTable: {e}")
        person_rm_session.rollback()
    finally:
        person_rm_session.close()


def main():
    setup_logging()
    logging.info("Connecting to databases...")
    DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH = find_database_paths()

    # Connect to DNAGedcom and RootsMagic databases using SQLAlchemy
    dg_session, dg_engine, rm_session, rm_engine = connect_to_db_sqlalchemy(DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH)

    if not all([dg_session, dg_engine, rm_session, rm_engine]):
        logging.critical("Failed to connect to one or both databases using SQLAlchemy.")
        return

    logging.info("Fetching user kit data...")
    dna_kits = user_kit_data(dg_session)

    if dna_kits:
        logging.info("Prompting user for kits...")
        selected_kits = prompt_user_for_kits(dna_kits)

        logging.info("Selected kits:")
        for kit in selected_kits:
            logging.info(f"Type: {kit[0]}, GUID: {kit[1]}, Name: {kit[2]} {kit[3]}")

        logging.info("Filtering selected kits...")
        filtered_ids = filter_selected_kits(dg_session, selected_kits)

        logging.info("Processing data...")
        processed_data = (
                process_ancestry(dg_session, filtered_ids) +
                process_ftdna(dg_session, filtered_ids) +
                process_mh(dg_session, filtered_ids)
        )

        logging.info("Inserting processed data into RootsMagic database...")
        insert_person(rm_session, processed_data)

    else:
        logging.warning("No kits found.")

    # Close sessions and engines
    dg_session.close()
    dg_engine.dispose()
    rm_session.close()
    rm_engine.dispose()


if __name__ == "__main__":
    main()
