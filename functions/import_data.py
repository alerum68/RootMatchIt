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
        # Example for AncestryMatchGroups table
        ancestry_matches = filter_session.query(Ancestry_matchGroups).filter(
            Ancestry_matchGroups.testGuid.in_(selected_guids)).all()
        test_ids['Ancestry_matchGroups'] = [match.Id for match in ancestry_matches]

        # Example for FTDNA_Matches2 table
        ftdna_matches = filter_session.query(FTDNA_Matches2).filter(
            FTDNA_Matches2.eKit1.in_(selected_guids) | FTDNA_Matches2.eKit2.in_(selected_guids)).all()
        test_ids['FTDNA_Matches2'] = [match.Id for match in ftdna_matches]

        # Example for MH_Match table
        mh_matches = filter_session.query(MH_Match).filter(MH_Match.guid.in_(selected_guids)).all()
        test_ids['MH_Match'] = [match.Id for match in mh_matches]

        # Extend for other tables as needed

        if test_ids:
            logger.info(f"Test IDs found: {test_ids}")
    except Exception as filter_e:
        logger.error(f"Error filtering selected kits: {filter_e}")

    return test_ids


def insert_person(person_dg_session: Session, person_rm_session: Session, person_filtered_ids):

    try:
        # Ancestry
        if person_filtered_ids.get('Ancestry_matchGroups'):
            ancestry_individuals = person_dg_session.query(Ancestry_matchGroups.testGuid,
                                                           Ancestry_matchGroups.subjectGender,
                                                           Ancestry_matchGroups.matchGuid).filter(
                Ancestry_matchGroups.Id.in_(person_filtered_ids['Ancestry_matchGroups'])
            ).all()

            for individual in ancestry_individuals:
                sex_value = 1 if individual.subjectGender == 'F' else 0 if individual.subjectGender == 'M' else 2
                check_for_duplicates(person_rm_session,
                                     individual.matchGuid,
                                     Sex=sex_value,
                                     Color=25,
                                     )
            logging.info(f"Processed {len(ancestry_individuals)} Ancestry individuals.")
        else:
            logging.info(f"Skipping Ancestry processing due to empty filtered IDs.")

        # FTDNA
        if person_filtered_ids.get('FTDNA_Matches2'):
            ftdna_individuals = person_dg_session.query(FTDNA_Matches2.Female,
                                                        FTDNA_Matches2.Name, FTDNA_Matches2.eKit2).filter(
                FTDNA_Matches2.Id.in_(person_filtered_ids['FTDNA_Matches2'])
            ).all()

            for individual in ftdna_individuals:
                unique_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, individual.Name))
                sex_value = 1 if individual.Female == 'true' else 0 if individual.Female == 'false' else 2
                check_for_duplicates(person_rm_session,
                                     unique_id,
                                     Sex=sex_value,
                                     Color=3,
                                     )
            logging.info(f"Processed {len(ftdna_individuals)} FTDNA individuals.")
        else:
            logging.info("Skipping FTDNA processing due to empty filtered IDs.")

        # MyHeritage
        if person_filtered_ids.get('MH_Match'):
            mh_individuals = person_dg_session.query(MH_Match.testid, MH_Match.gender).filter(
                MH_Match.Id.in_(person_filtered_ids['MH_Match'])
            ).all()

            for individual in mh_individuals:
                sex_value = 1 if individual.gender == 'F' else 0 if individual.gender == 'M' else 2
                check_for_duplicates(person_rm_session,
                                     individual.testid,
                                     Sex=sex_value,
                                     Color=17,
                                     )
            logging.info(f"Processed {len(mh_individuals)} MyHeritage individuals.")
        else:
            logging.info("Skipping MyHeritage processing due to empty filtered IDs.")

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
