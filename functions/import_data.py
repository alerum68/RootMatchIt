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


def generate_unique_id(*args) -> str:
    # Filter out empty or None values from the arguments
    filtered_args = [str(arg) for arg in args if arg]
    if not filtered_args:
        return str(uuid.uuid4())  # Return a random UUID if all args are empty
    unique_str = ' '.join(filtered_args)  # Combine all arguments into a single string
    namespace = uuid.NAMESPACE_DNS  # Use a predefined namespace, or create your own using uuid.uuid4()
    unique_id = str(uuid.uuid5(namespace, unique_str))
    return unique_id


def check_for_duplicates(session: Session, unique_id: str, **kwargs):
    logger = logging.getLogger('get_or_create_person')
    try:
        person = session.query(PersonTable).filter_by(UniqueID=unique_id).first()
        if person:
            # Update existing person with new data
            for key, value in kwargs.items():
                setattr(person, key, value)
            logger.info(f"Updated existing person: {unique_id}")
        else:
            # Create new person
            person = PersonTable(UniqueID=unique_id, **kwargs)
            session.add(person)
            logger.info(f"Created new person: {unique_id}")

        return person
    except SQLAlchemyError as dup_e:
        logger.error(f"Error in get_or_create_person for {unique_id}: {dup_e}")
        session.rollback()
        return None


def filter_selected_kits(filter_session: Session, f_selected_kits):
    logger = logging.getLogger('filter_selected_kits')
    logger.info("Filtering selected kits...")

    selected_guids = [kit[1] for kit in f_selected_kits]  # Extracting GUIDs from selected kits
    test_ids = {}

    try:
        # Example for Ancestry_matchGroups table
        ancestry_matches = filter_session.query(Ancestry_matchGroups).filter(
            Ancestry_matchGroups.testGuid.in_(selected_guids)).all()
        test_ids['Ancestry_matchGroups'] = [match.Id for match in ancestry_matches]

        logger.info("Processing Ancestry_matchTrees with selected GUIDs: %s", selected_guids)

        # Step 1: Retrieve matchGuids from Ancestry_matchGroups
        match_guids = [group.matchGuid for group in ancestry_matches]
        logger.info("Found matchGuids in Ancestry_matchGroups: %s", match_guids)

        # Step 2: Filter Ancestry_matchTrees based on matchGuids (previously matchid)
        ancestry_matches_trees = filter_session.query(Ancestry_matchTrees).filter(
            Ancestry_matchTrees.matchid.in_(match_guids)).all()

        matched_ids = [match.Id for match in ancestry_matches_trees]
        logger.info("Matched IDs found in Ancestry_matchTrees: %s", matched_ids)

        test_ids['Ancestry_matchTrees'] = matched_ids

        # Example for FTDNA_Matches2 table
        ftdna_matches = filter_session.query(FTDNA_Matches2).filter(
            FTDNA_Matches2.eKit1.in_(selected_guids) | FTDNA_Matches2.eKit2.in_(selected_guids)).all()
        test_ids['FTDNA_Matches2'] = [match.Id for match in ftdna_matches]

        # Example for MH_Match table
        mh_matches = filter_session.query(MH_Match).filter(
            MH_Match.guid.in_(selected_guids)).all()
        test_ids['MH_Match'] = [match.Id for match in mh_matches]

        # Extend for other tables as needed

        if not any(test_ids.values()):
            test_ids = {}  # Clear the dictionary if no matches are found

        if test_ids:
            logger.info(f"Test IDs found: {test_ids}")
        else:
            logger.info("No test IDs found.")

    except Exception as filter_e:
        logger.error(f"Error filtering selected kits: {filter_e}")

    return test_ids


def insert_person(person_dg_session: Session, person_rm_session: Session, person_filtered_ids, batch_size=1000):
    try:
        # Ancestry
        if person_filtered_ids.get('Ancestry_matchGroups'):
            ancestry_individuals_groups = person_dg_session.query(
                Ancestry_matchGroups.testGuid,
                Ancestry_matchGroups.subjectGender,
                Ancestry_matchGroups.matchGuid
            ).filter(
                Ancestry_matchGroups.Id.in_(person_filtered_ids['Ancestry_matchGroups'])
            ).yield_per(batch_size)

            count = 0
            for individual in ancestry_individuals_groups:
                sex_value = 1 if individual.subjectGender == 'F' else 0 if individual.subjectGender == 'M' else 2
                check_for_duplicates(person_rm_session,
                                     individual.matchGuid,
                                     Sex=sex_value,
                                     Color=25,
                                     )
                count += 1
                if count % batch_size == 0:
                    person_rm_session.flush()  # Flush changes to the database

            logging.info(f"Processed {count} Ancestry individuals from matchGroups.")
            person_rm_session.commit()
        else:
            logging.info(f"Skipping Ancestry matchGroups processing due to empty filtered IDs.")

        if person_filtered_ids.get('Ancestry_matchTrees'):
            ancestry_individuals_trees = person_dg_session.query(
                Ancestry_matchTrees.matchid,
                Ancestry_matchTrees.given,
                Ancestry_matchTrees.surname,
                Ancestry_matchTrees.birthdate
            ).filter(
                Ancestry_matchTrees.Id.in_(person_filtered_ids['Ancestry_matchTrees'])
            ).yield_per(batch_size)

            count = 0
            for individual in ancestry_individuals_trees:
                try:
                    unique_id = generate_unique_id(individual.given, individual.surname, individual.birthdate)
                    check_for_duplicates(person_rm_session,
                                         unique_id,
                                         Sex=2,  # Adjust as per your data
                                         Color=25,  # Adjust as per your data
                                         )
                    count += 1
                    if count % batch_size == 0:
                        person_rm_session.flush()  # Flush changes to the database
                except Exception as ap_e:
                    logging.error(f"Error generating unique ID for individual: {ap_e}")
                    continue

            logging.info(f"Processed {count} Ancestry individuals from matchTrees.")
            person_rm_session.commit()
        else:
            logging.info(f"Skipping Ancestry matchTrees processing due to empty filtered IDs.")

        # Commit final changes if any records remain
        person_rm_session.commit()
        logging.info("Successfully inserted or updated individuals in PersonTable.")
    except Exception as per_e:
        logging.error(f"Error inserting or updating PersonTable: {per_e}")
        person_rm_session.rollback()
    finally:
        person_rm_session.close()
        person_dg_session.close()


if __name__ == '__main__':
    setup_logging()

    dg_session = None
    rm_session = None
    dg_engine = None
    rm_engine = None

    try:
        DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH = find_database_paths()
        dg_session, dg_engine, rm_session, rm_engine = connect_to_db_sqlalchemy(DNAGEDCOM_DB_PATH, ROOTSMAGIC_DB_PATH)
        dna_kits = user_kit_data(dg_session)
        if dna_kits:
            selected_kits = prompt_user_for_kits(dna_kits)
            filtered_ids = filter_selected_kits(dg_session, selected_kits)
            insert_person(dg_session, rm_session, filtered_ids)
    except Exception as e:
        logging.critical(f"Critical error in main execution: {e}")
    finally:
        if dg_session is not None:
            dg_session.close()
        if rm_session is not None:
            rm_session.close()
        if dg_engine is not None:
            dg_engine.dispose()
        if rm_engine is not None:
            rm_engine.dispose()
