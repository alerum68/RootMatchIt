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
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, MultipleResultsFound
import uuid
import hashlib

ancestry_base = Ancestry_Base()
ftdna_base = FTDNA_Base()
mh_base = MH_Base()
rm_base = RM_Base()

# Switches
limit = 500
# Ancestry
ancestry_matchgroups = 1
ancestry_matchtrees = 1
ancestry_treedata = 1
ancestry_icw = 1
ancestry_ancestorcouple = 1
ancestry_matchethnicity = 1
# FTDNA
ftdna_matches2 = 0
ftdna_chromo2 = 0
ftdna_icw2 = 0
dg_tree = 0
dg_individual = 0
# MyHeritage
mh_match = 0
mh_ancestors = 0
mh_chromo = 0
mh_icw = 0
mh_tree = 0


# Set a batch limit via the Limit global variable
def batch_limit(session, table_class, filter_ids, process_func, apply_limit):
    query = session.query(table_class).filter(table_class.Id.in_(filter_ids))
    if apply_limit > 0:
        query = query.limit(apply_limit)
    limited_data = query.all()

    # Fetch related person data for Ancestry_matchTrees
    processed_data = []
    for item in limited_data:
        if isinstance(item, Ancestry_matchTrees):
            person_data = session.query(Ancestry_matchTrees).filter_by(Id=item.Id).first()
            processed_data.append(process_func(item, person_data))
        else:
            processed_data.append(process_func(item))

    return processed_data[:apply_limit] if apply_limit > 0 else processed_data


# Generate UUIDs
def generate_unique_id(*args) -> str:
    filtered_args = [str(arg) for arg in args if arg]
    unique_str = ' '.join(filtered_args)
    namespace = uuid.NAMESPACE_DNS
    unique_id = str(uuid.uuid5(namespace, unique_str))
    return unique_id


# Generate a hashed version of PersonID, FatherID, and MotherID to fix DnaGedcom ID size
def hash_id(original_id, id_mapping):
    if original_id is None:
        return None
    if original_id in id_mapping:
        return id_mapping[original_id]
    hash_object = hashlib.md5(str(original_id).encode())
    hash_hex = hash_object.hexdigest()
    hash_int = int(hash_hex[:8], 16)
    hashed_id = (hash_int % 9999999) + 100
    id_mapping[original_id] = hashed_id
    # logging.debug(f"Hashing ID: original_id={original_id}, hashed_id={hashed_id}")
    return hashed_id


# A check for duplicate records.
def check_for_duplicates(session: Session, unique_id: str, **kwargs):
    logging.getLogger('get_or_create_person')
    try:
        person = session.query(PersonTable).filter_by(UniqueID=unique_id).first()
        if person:
            for key, value in kwargs.items():
                setattr(person, key, value)
            logging.info(f"Updated existing person: {unique_id}")
        else:
            person = PersonTable(UniqueID=unique_id, **kwargs)
            session.add(person)
            logging.info(f"Created new person: {unique_id}")
        return person
    except SQLAlchemyError as dup_e:
        logging.error(f"Error in get_or_create_person for {unique_id}: {dup_e}")
        session.rollback()
        return None


# Filter results based on kits selected via select_kits function.
def filter_selected_kits(filter_session: Session, f_selected_kits):
    global ancestry_matchgroups, ancestry_matchtrees, ancestry_treedata, ancestry_icw, \
        ancestry_ancestorcouple, ancestry_matchethnicity
    global ftdna_matches2, ftdna_chromo2, ftdna_icw2, dg_tree, dg_individual
    global mh_match, mh_ancestors, mh_chromo, mh_icw, mh_tree
    logging.getLogger('filter_selected_kits')
    logging.info("Filtering selected kits...")

    selected_guids = [kit[1] for kit in f_selected_kits]
    test_ids = {
        'Ancestry_matchGroups': [], 'Ancestry_matchTrees': [], 'Ancestry_TreeData': [],
        'Ancestry_ICW': [], 'AncestryAncestorCouple': [], 'Ancestry_matchEthnicity': [],
        'FTDNA_Matches2': [], 'FTDNA_Chromo2': [], 'FTDNA_ICW2': [],
        'DGTree': [], 'DGIndividual': [],
        'MH_Match': [], 'MH_Ancestors': [], 'MH_Chromo': [], 'MH_ICW': [], 'MH_Tree': []
    }

    try:
        # Ancestry filters
        if ancestry_matchgroups:
            ancestry_matches = filter_session.query(Ancestry_matchGroups).filter(
                Ancestry_matchGroups.testGuid.in_(selected_guids)).all()
            test_ids['Ancestry_matchGroups'] = [match.Id for match in ancestry_matches]

            # Get matchGuids for use in ancestry_matchtrees
            match_guids = [group.matchGuid for group in ancestry_matches]

            if ancestry_matchtrees:
                # Use both selected_guids and match_guids for ancestry_matchtrees
                guids_to_check = selected_guids + match_guids
                ancestry_matches_trees = filter_session.query(Ancestry_matchTrees).filter(
                    Ancestry_matchTrees.matchid.in_(guids_to_check)).all()
                test_ids['Ancestry_matchTrees'] = [match.Id for match in ancestry_matches_trees]

        if ancestry_treedata:
            ancestry_tree_data = filter_session.query(Ancestry_TreeData).filter(
                Ancestry_TreeData.TestGuid.in_(selected_guids)).all()
            test_ids['Ancestry_TreeData'] = [data.Id for data in ancestry_tree_data]

        if ancestry_icw:
            ancestry_icw_data = filter_session.query(Ancestry_ICW).filter(
                Ancestry_ICW.matchid.in_(selected_guids)).all()
            test_ids['Ancestry_ICW'] = [icw.Id for icw in ancestry_icw_data]

        if ancestry_ancestorcouple:
            ancestry_ancestor_couple = filter_session.query(AncestryAncestorCouple).filter(
                AncestryAncestorCouple.TestGuid.in_(selected_guids)).all()
            test_ids['AncestryAncestorCouple'] = [couple.Id for couple in ancestry_ancestor_couple]

        if ancestry_matchethnicity:
            ancestry_match_ethnicity = filter_session.query(Ancestry_matchEthnicity).filter(
                Ancestry_matchEthnicity.matchGuid.in_(selected_guids)).all()
            test_ids['Ancestry_matchEthnicity'] = [ethnicity.Id for ethnicity in ancestry_match_ethnicity]

        # FTDNA filters
        if ftdna_matches2 or ftdna_chromo2 or ftdna_icw2 or dg_tree or dg_individual:
            if ftdna_matches2:
                ftdna_matches = filter_session.query(FTDNA_Matches2).filter(
                    FTDNA_Matches2.eKit1.in_(selected_guids)).all()
                test_ids['FTDNA_Matches2'] = [match.Id for match in ftdna_matches]

            if ftdna_chromo2:
                ftdna_chromo = filter_session.query(FTDNA_Chromo2).filter(
                    FTDNA_Chromo2.eKit1.in_(selected_guids)).all()
                test_ids['FTDNA_Chromo2'] = [chromo.Id for chromo in ftdna_chromo]

            if ftdna_icw2:
                ftdna_icw = filter_session.query(FTDNA_ICW2).filter(
                    FTDNA_ICW2.eKitKit.in_(selected_guids)).all()
                test_ids['FTDNA_ICW2'] = [icw.Id for icw in ftdna_icw]

            if dg_tree:
                dg_trees = filter_session.query(DGTree).filter(
                    DGTree.matchID.in_(selected_guids)).all()
                test_ids['DGTree'] = [tree.Id for tree in dg_trees]

            if dg_individual:
                dg_individuals = filter_session.query(DGIndividual).filter(
                    DGIndividual.matchid.in_(selected_guids)).all()
                test_ids['DGIndividual'] = [individual.Id for individual in dg_individuals]

        # MyHeritage filters
        if mh_match or mh_ancestors or mh_chromo or mh_icw or mh_tree:
            if mh_match:
                mh_matches = filter_session.query(MH_Match).filter(
                    MH_Match.guid.in_(selected_guids)).all()
                test_ids['MH_Match'] = [match.Id for match in mh_matches]

            if mh_ancestors:
                mh_ancestors_data = filter_session.query(MH_Ancestors).filter(
                    MH_Ancestors.matchid.in_(selected_guids)).all()
                test_ids['MH_Ancestors'] = [ancestor.Id for ancestor in mh_ancestors_data]

            if mh_chromo:
                mh_chromo_data = filter_session.query(MH_Chromo).filter(
                    MH_Chromo.guid.in_(selected_guids)).all()
                test_ids['MH_Chromo'] = [chromo.Id for chromo in mh_chromo_data]

            if mh_icw:
                mh_icw_data = filter_session.query(MH_ICW).filter(
                    MH_ICW.id1.in_(selected_guids)).all()
                test_ids['MH_ICW'] = [icw.Id for icw in mh_icw_data]

            if mh_tree:
                mh_tree_data = filter_session.query(MH_Tree).all()
                test_ids['MH_Tree'] = [tree.Id for tree in mh_tree_data]

    except Exception as filter_e:
        logging.error(f"Error filtering selected kits: {filter_e}")
        raise

    return test_ids


# Create the DNA Kit FactType if not present.  For future integration with existing tree.
def insert_fact_type(fact_rm_session: Session):
    logging.getLogger('insert_fact_type')

    try:
        # Check if Fact Type 'DNA Kit' already exists
        fact_type = fact_rm_session.query(FactTypeTable).filter_by(Name='DNA Kit').first()

        if not fact_type:
            # Create a new Fact Type 'DNA Kit' if it doesn't exist
            new_fact_type = FactTypeTable(
                OwnerType=0,
                Name='DNA Kit',
                Abbrev='DNA Kit',
                GedcomTag='EVEN',
                UseValue=1,
                UseDate=0,
                UsePlace=0,
                Sentence='[person] had a DNA test performed. View DNA Tab in profile to view matches.',
                Flags=2147483647,
                UTCModDate=func.julianday('now') - 2415018.5,
            )
            fact_rm_session.add(new_fact_type)
            fact_rm_session.commit()
            logging.info("Fact Type 'DNA Kit' inserted into FactTypeTable.")
        else:
            # Update existing Fact Type 'DNA Kit'
            fact_type.OwnerType = 0
            fact_type.Abbrev = 'DNA Kit'
            fact_type.GedcomTag = 'EVEN'
            fact_type.UseValue = 1
            fact_type.UseDate = 0
            fact_type.UsePlace = 0
            fact_type.Sentence = '[person] had a DNA test performed. View DNA Tab in profile to view matches.'
            fact_type.Flags = 2147483647
            fact_type.UTCModDate = func.julianday('now') - 2415018.5
            fact_rm_session.commit()
            logging.info("Fact Type 'DNA Kit' updated in FactTypeTable.")
    except Exception as e:
        logging.error(f"Error inserting or updating Fact Type 'DNA Kit' in FactTypeTable: {e}")
        fact_rm_session.rollback()
        raise
    finally:
        fact_rm_session.close()


# Import Ancestry Profiles and FTDNA and MyHeritage DNA Kits.
def import_profiles(rm_session: Session, selected_kits):
    def get_gender_input(profile_name_given, profile_name_surname, profile_guid_value):
        # Manually enter gender for each Profile or DNA Kit.
        while True:
            gender = input(f"Enter gender for kit {profile_name_given} {profile_name_surname}, "
                           f"GUID {profile_guid_value} ((M)ale, (F)emale, (U)nknown): ").strip().upper()
            if gender in ['M', 'MALE']:
                return 0
            elif gender in ['F', 'FEMALE']:
                return 1
            elif gender in ['U', 'UNKNOWN']:
                return 2
            else:
                print("Invalid input. Please enter M/Male, F/Female, or U/Unknown.")

    for kit_details in selected_kits:
        id_value, guid_value, name_given, name_surname = kit_details

        try:
            # Attempt to fetch existing person record by UniqueID
            person_record = rm_session.query(PersonTable).filter_by(UniqueID=guid_value).one_or_none()

            if person_record:
                # Use existing gender value if it is already set
                if person_record.Sex not in [0, 1]:
                    gender_value = get_gender_input(name_given, name_surname, guid_value)
                else:
                    gender_value = person_record.Sex

                # Update existing record with new gender value if needed
                if person_record.Sex != gender_value:
                    person_record.Sex = gender_value
                    person_record.UTCModDate = func.julianday('now') - 2415018.5

            else:
                gender_value = get_gender_input(name_given, name_surname, guid_value)
                person_record = PersonTable(
                    UniqueID=guid_value,
                    Color=1,
                    UTCModDate=func.julianday('now') - 2415018.5,
                    Sex=gender_value
                )
                rm_session.add(person_record)

            rm_session.commit()

            # Retrieve PersonID and process NameTable records
            person_id = person_record.PersonID
            existing_name = rm_session.query(NameTable).filter_by(OwnerID=person_id, NameType=6).first()

            if existing_name:
                # Update existing name record
                existing_name.Given = name_given
                existing_name.Surname = name_surname
                existing_name.IsPrimary = 1
            else:
                # Create a new name record in NameTable
                profile_name_table = NameTable(
                    OwnerID=person_id,
                    Given=name_given,
                    Surname=name_surname,
                    IsPrimary=1,
                    NameType=6,
                )
                rm_session.add(profile_name_table)

        except MultipleResultsFound:
            logging.error(f"Multiple records found for UniqueID: {guid_value}. "
                          f"Please check the database for duplicates.")
            rm_session.rollback()
        except Exception as e:
            logging.error(f"Error processing kit: {id_value}, GUID: {guid_value}, Name: {name_given} {name_surname}")
            logging.exception(e)
            rm_session.rollback()

    rm_session.commit()
    logging.info("Inserted or updated selected Profiles.")


# Process Ancestry data
def process_ancestry(session: Session, filtered_ids):
    global limit
    logging.getLogger('process_ancestry')
    logging.info("Processing Ancestry data...")

    processed_ancestry_data = []
    id_mapping = {}  # Initialize id_mapping dictionary

    try:
        id_mapping = {}
        # Process Ancestry_MatchGroups data
        if ancestry_matchgroups and filtered_ids.get('Ancestry_matchGroups'):
            def process_matchgroup(mg_session, group):
                # Query Ancestry_matchTrees using matchGuid
                match_tree = mg_session.query(Ancestry_matchTrees).filter_by(matchid=group.matchGuid).first()

                # create hashed ids for PersonID, FatherID, and MotherID
                if match_tree and match_tree.personId is not None:
                    person_id = hash_id(match_tree.personId, id_mapping)
                    father_id = hash_id(match_tree.fatherId, id_mapping) if match_tree.fatherId is not None else None
                    mother_id = hash_id(match_tree.motherId, id_mapping) if match_tree.motherId is not None else None
                    color = 18
                else:
                    # If no match_tree is found, generate a default person_id using matchGuid and set color to 27
                    person_id = hash_id(group.matchGuid, id_mapping)
                    father_id = None
                    mother_id = None
                    color = 27

                # Extract name and sex information
                name = group.matchTestDisplayName
                given, surname = (name.split()[0], name.split()[-1]) if len(name.split()) > 1 else (name, "")

                subject_gender = group.subjectGender
                sex = 1 if subject_gender == 'F' else 0 if subject_gender == 'M' else 2  # Default or unknown value

                return {
                    'source': 'process_matchgroup',
                    'DNAProvider': 2,
                    'PersonID': person_id,
                    'FatherID': father_id,
                    'MotherID': mother_id,
                    'unique_id': group.matchGuid,
                    'sex': sex,
                    'color': color,
                    'matchTestDisplayName': group.matchTestDisplayName,
                    'Given': given,
                    'Surname': surname,
                    'groupName': group.groupName,
                    'confidence': group.confidence,
                    'sharedCM': group.sharedCentimorgans,
                    'SharedSegs': group.sharedSegment,
                    'starred': group.starred,
                    'note': group.note,
                    'matchTreeId': group.matchTreeId,
                    'treeId': group.treeId,
                    'icwRunDate': group.icwRunDate,
                    'treeRunDate': group.treeRunDate,
                    'matchRunDate': group.matchRunDate,
                    'paternal': group.paternal,
                    'maternal': group.maternal,
                    'subjectGender': group.subjectGender,
                    'meiosisValue': group.meiosisValue,
                    'parentCluster': group.parentCluster,
                    'IsPrimary': 1,
                    'NameType': 6,
                }

            match_groups = batch_limit(
                session, Ancestry_matchGroups, filtered_ids.get('Ancestry_matchGroups', []),
                lambda group: process_matchgroup(session, group), limit
            )

            id_mapping = {group['unique_id']: group for group in match_groups}
            processed_ancestry_data.extend(match_groups or [])

        # Process Ancestry_MatchTrees data
        if ancestry_matchtrees and filtered_ids.get('Ancestry_matchTrees'):
            def process_matchtree(tree, person_data=None):
                try:
                    data_source = person_data if person_data else tree

                    # Get IDs
                    person_id = hash_id(data_source.personId, id_mapping)
                    father_id = hash_id(data_source.fatherId, id_mapping) if data_source.fatherId is not None else None
                    mother_id = hash_id(data_source.motherId, id_mapping) if data_source.motherId is not None else None

                    # Determine gender
                    sex_value = 2
                    if data_source.personId is not None:
                        father_match = session.query(Ancestry_matchTrees).filter(
                            Ancestry_matchTrees.fatherId == data_source.personId
                        ).first()
                        if father_match:
                            sex_value = 0
                        else:
                            mother_match = session.query(Ancestry_matchTrees).filter(
                                Ancestry_matchTrees.motherId == data_source.personId
                            ).first()
                            if mother_match:
                                sex_value = 1

                    unique_id = tree.matchid
                    match_group_data = id_mapping.get(unique_id, {})

                    # Update name and gender based on match_group_data
                    if tree.relid == '1':
                        surname = match_group_data.get('Surname', tree.surname)
                        given = match_group_data.get('Given', tree.given)
                        name_type = match_group_data.get('NameType', 6)
                        sex_value = match_group_data.get('sex', sex_value)

                        # Ensure FatherID and MotherID are included
                        father_id = hash_id(tree.fatherId, id_mapping) if tree.fatherId is not None else father_id
                        mother_id = hash_id(tree.motherId, id_mapping) if tree.motherId is not None else mother_id
                    else:
                        unique_id = generate_unique_id(tree.given, tree.surname, tree.matchid, tree.relid)
                        surname = tree.surname
                        given = tree.given
                        name_type = 2

                    return {
                        'source': 'process_matchtree',
                        'unique_id': unique_id,
                        'sex': sex_value,
                        'color': 24,
                        'matchid': tree.matchid,
                        'Surname': surname,
                        'Given': given,
                        'birthdate': tree.birthdate,
                        'deathdate': tree.deathdate,
                        'birthplace': tree.birthplace,
                        'deathplace': tree.deathplace,
                        'relid': tree.relid,
                        'PersonID': person_id,
                        'FatherID': father_id,
                        'MotherID': mother_id,
                        'DNAProvider': 2,
                        'Date': tree.created_date,
                        'IsPrimary': 1,
                        'NameType': name_type,
                    }

                except ValueError as mte:
                    logging.warning(f"Invalid ID value for tree {tree.matchid}: {str(mte)}. Skipping.")
                    return None

            try:
                # Process match trees with limit and process_matchtree function
                match_trees = batch_limit(
                    session, Ancestry_matchTrees, filtered_ids.get('Ancestry_matchTrees', []),
                    process_matchtree, limit
                )

                # Extend processed ancestry data
                processed_ancestry_data.extend(match_trees or [])
            except Exception as e:
                logging.error(f"An error occurred while processing Ancestry match trees: {str(e)}")
                logging.exception("Exception details:")

        # Process Ancestry_TreeData data
        if ancestry_treedata and filtered_ids.get('Ancestry_TreeData'):
            def process_treedata(treedata):
                try:
                    return {
                        'source': 'process_treedata',
                        'TestGuid': treedata.TestGuid,
                        'TreeSize': treedata.TreeSize,
                        'PublicTree': treedata.PublicTree,
                        'PrivateTree': treedata.PrivateTree,
                        'UnlinkedTree': treedata.UnlinkedTree,
                        'TreeId': treedata.TreeId,
                        'NoTrees': treedata.NoTrees,
                        'TreeUnavailable': treedata.TreeUnavailable,
                    }
                except AttributeError as tde:
                    logging.warning(f"Missing attribute in tree data: {str(tde)}. Skipping this record.")
                    return None

            try:
                # Process Ancestry tree data
                tree_data = batch_limit(
                    session, Ancestry_TreeData, filtered_ids.get('Ancestry_TreeData', []),
                    process_treedata, limit
                )

                processed_ancestry_data.extend(tree_data or [])
            except Exception as e:
                logging.error(f"An error occurred while processing Ancestry tree data: {str(e)}")
                logging.exception("Exception details:")

        # Process Ancestry_ICW data
        if ancestry_icw and filtered_ids.get('Ancestry_ICW'):
            def process_icw(icw):
                match_tree = session.query(Ancestry_matchTrees).filter_by(matchid=icw.matchid).first()
                person_id = hash_id(match_tree.personId,
                                    id_mapping) if match_tree and match_tree.personId is not None else None

                return {
                    'source': 'process_icw',
                    'PersonID': person_id,
                    'testGuid': icw.matchid,
                    'matchGuid': icw.icwid,
                    'Date': icw.created_date,
                    'sharedCM': icw.sharedCentimorgans,
                    'confidence': icw.confidence,
                    'meiosis': icw.meiosis,
                    'numSharedSegments': icw.numSharedSegments,
                }

            try:
                # Process Ancestry ICW data
                icw_data = batch_limit(
                    session, Ancestry_ICW, filtered_ids.get('Ancestry_ICW', []),
                    process_icw, limit
                )

                processed_ancestry_data.extend(icw_data or [])
            except Exception as e:
                logging.error(f"An error occurred while processing Ancestry ICW data: {str(e)}")
                logging.exception("Exception details:")

        # Process Ancestry_matchEthnicity
        if ancestry_matchethnicity and filtered_ids.get('Ancestry_matchEthnicity'):
            def process_matchethnicity(ethnicity):
                try:
                    return {
                        'source': 'process_matchethnicity',
                        'unique_id': generate_unique_id(ethnicity.matchGuid),
                        'matchGuid': ethnicity.matchGuid,
                        'ethnicregions': ethnicity.ethnicregions,
                        'ethnictraceregions': ethnicity.ethnictraceregions,
                        'Date': ethnicity.created_date,
                        'percent': ethnicity.percent,
                        'version': ethnicity.version,
                    }
                except AttributeError as mee:
                    logging.warning(f"Missing attribute in match ethnicity: {str(mee)}. Skipping this record.")
                    return None

            try:
                # Process Ancestry match ethnicity data
                match_ethnicity_data = batch_limit(
                    session, Ancestry_matchEthnicity, filtered_ids.get('Ancestry_matchEthnicity', []),
                    process_matchethnicity, limit
                )

                processed_ancestry_data.extend(match_ethnicity_data or [])
            except Exception as e:
                logging.error(f"An error occurred while processing Ancestry match ethnicity data: {str(e)}")
                logging.exception("Exception details:")

        # Process AncestryAncestorCouple
        if ancestry_ancestorcouple and filtered_ids.get('AncestryAncestorCouple'):
            def process_ancestorcouple(couple):
                try:
                    return {
                        'source': 'process_ancestorcouple',
                        'Id': couple.Id,
                        'TestGuid': couple.TestGuid,
                        'MatchGuid': couple.MatchGuid,
                        'FatherAmtGid': couple.FatherAmtGid,
                        'FatherBigTreeGid': couple.FatherBigTreeGid,
                        'FatherKinshipPathToSampleId': couple.FatherKinshipPathToSampleId,
                        'FatherKinshipPathFromSampleToMatch': couple.FatherKinshipPathFromSampleToMatch,
                        'FatherPotential': couple.FatherPotential,
                        'FatherInMatchTree': couple.FatherInMatchTree,
                        'FatherInBestContributorTree': couple.FatherInBestContributorTree,
                        'FatherDisplayName': couple.FatherDisplayName,
                        'FatherBirthYear': couple.FatherBirthYear,
                        'FatherDeathYear': couple.FatherDeathYear,
                        'FatherIsMale': couple.FatherIsMale,
                        'FatherNotFound': couple.FatherNotFound,
                        'FatherVeiled': couple.FatherVeiled,
                        'FatherRelationshipToSampleId': couple.FatherRelationshipToSampleId,
                        'FatherRelationshipFromSampleToMatch': couple.FatherRelationshipFromSampleToMatch,
                        'MotherAmtGid': couple.MotherAmtGid,
                        'MotherBigTreeGid': couple.MotherBigTreeGid,
                        'MotherKinshipPathToSampleId': couple.MotherKinshipPathToSampleId,
                        'MotherKinshipPathFromSampleToMatch': couple.MotherKinshipPathFromSampleToMatch,
                        'MotherPotential': couple.MotherPotential,
                        'MotherInMatchTree': couple.MotherInMatchTree,
                        'MotherInBestContributorTree': couple.MotherInBestContributorTree,
                        'MotherDisplayName': couple.MotherDisplayName,
                        'MotherBirthYear': couple.MotherBirthYear,
                        'MotherDeathYear': couple.MotherDeathYear,
                        'MotherIsFemale': couple.MotherIsFemale,
                        'MotherNotFound': couple.MotherNotFound,
                        'MotherVeiled': couple.MotherVeiled,
                        'MotherRelationshipToSampleId': couple.MotherRelationshipToSampleId,
                        'MotherRelationshipFromSampleToMatch': couple.MotherRelationshipFromSampleToMatch,
                        'date': couple.date,
                    }
                except AttributeError as ace:
                    logging.warning(f"Missing attribute in ancestor couple: {str(ace)}. Skipping this record.")
                    return None

            try:
                # Process Ancestry ancestor couple data
                ancestor_couple_data = batch_limit(
                    session, AncestryAncestorCouple, filtered_ids.get('AncestryAncestorCouple', []),
                    process_ancestorcouple, limit
                )

                processed_ancestry_data.extend(ancestor_couple_data or [])
            except Exception as e:
                logging.error(f"An error occurred while processing Ancestry ancestor couple data: {str(e)}")
                logging.exception("Exception details:")

    except Exception as e:
        logging.error(f"An error occurred during processing Ancestry data: {str(e)}")

    return processed_ancestry_data


# Process FTDNA data
def process_ftdna(session: Session, filtered_ids):
    global limit
    logging.getLogger('process_ftdna')
    logging.info("Processing FTDNA data...")

    processed_ftdna_data = []

    try:
        if ftdna_matches2 and filtered_ids.get('FTDNA_Matches2'):
            def process_ftdna_match(match):
                return {
                    'unique_id': generate_unique_id(match.eKit1, match.eKit2),
                    'sex': 1 if match.Female else 0,
                    'color': 26,
                    'Name': match.Name,
                    'MatchPersonName': match.MatchPersonName,
                    'Email': match.Email,
                    'Relationship': match.Relationship,
                    'totalCM': match.totalCM,
                    'longestCentimorgans': match.longestCentimorgans,
                    'yHaplo': match.yHaplo,
                    'mtHaplo': match.mtHaplo,
                }

            processed_ftdna_data.extend(batch_limit(
                session, FTDNA_Matches2, filtered_ids['FTDNA_Matches2'],
                process_ftdna_match, limit
            ))

        if ftdna_chromo2 and filtered_ids.get('FTDNA_Chromo2'):
            def process_ftdna_chromo(chromo):
                return {
                    'unique_id': generate_unique_id(chromo.eKit1, chromo.eKit2, chromo.chromosome),
                    'color': 26,
                    'eKit1': chromo.eKit1,
                    'eKit2': chromo.eKit2,
                    'chromosome': chromo.chromosome,
                    'cmfloat': chromo.cmfloat,
                    'p1': chromo.p1,
                    'p2': chromo.p2,
                    'snpsI': chromo.snpsI,
                }

            processed_ftdna_data.extend(batch_limit(
                session, FTDNA_Chromo2, filtered_ids['FTDNA_Chromo2'],
                process_ftdna_chromo, limit
            ))

        if ftdna_icw2 and filtered_ids.get('FTDNA_ICW2'):
            def process_ftdna_icw(icw):
                return {
                    'unique_id': generate_unique_id(icw.eKitKit, icw.eKitMatch1, icw.eKitMatch2),
                    'color': 26,
                    'eKitKit': icw.eKitKit,
                    'eKitMatch1': icw.eKitMatch1,
                    'eKitMatch2': icw.eKitMatch2,
                }

            processed_ftdna_data.extend(batch_limit(
                session, FTDNA_ICW2, filtered_ids['FTDNA_ICW2'],
                process_ftdna_icw, limit
            ))

        if dg_tree and filtered_ids.get('DGTree'):
            def process_dg_tree(tree):
                return {
                    'unique_id': generate_unique_id(tree.treeid, tree.matchID),
                    'color': 26,
                    'name': tree.name,
                    'treeid': tree.treeid,
                    'treeurl': tree.treeurl,
                    'basePersonId': tree.basePersonId,
                    'matchID': tree.matchID,
                }

            processed_ftdna_data.extend(batch_limit(
                session, DGTree, filtered_ids['DGTree'],
                process_dg_tree, limit
            ))

        if dg_individual and filtered_ids.get('DGIndividual'):
            def process_dg_individual(individual):
                return {
                    'unique_id': generate_unique_id(individual.treeid, individual.matchid, individual.personId),
                    'sex': 1 if individual.sex == 'F' else 0 if individual.sex == 'M' else 2,
                    'color': 26,
                    'treeid': individual.treeid,
                    'matchid': individual.matchid,
                    'surname': individual.surname,
                    'given': individual.given,
                    'birthdate': individual.birthdate,
                    'deathdate': individual.deathdate,
                    'birthplace': individual.birthplace,
                    'deathplace': individual.deathplace,
                    'personId': individual.personId,
                    'fatherId': individual.fatherId,
                    'motherId': individual.motherId,
                }

            processed_ftdna_data.extend(batch_limit(
                session, DGIndividual, filtered_ids['DGIndividual'],
                process_dg_individual, limit
            ))

    except Exception as e:
        logging.error(f"Error processing FTDNA data: {e}")

    return processed_ftdna_data


# Process MyHeritage data
def process_mh(session: Session, filtered_ids):
    global limit
    logging.getLogger('process_mh')
    logging.info("Processing MyHeritage data...")

    processed_mh_data = []

    try:
        if mh_match and filtered_ids.get('MH_Match'):
            def process_mh_match(match):
                return {
                    'unique_id': generate_unique_id(match.guid),
                    'sex': 1 if match.gender == 'F' else 0 if match.gender == 'M' else 2,
                    'color': 27,
                    'name': match.name,
                    'first_name': match.first_name,
                    'last_name': match.last_name,
                    'estimated_relationship': match.estimated_relationship,
                    'totalCM': match.totalCM,
                    'percent_shared': match.percent_shared,
                    'num_segments': match.num_segments,
                    'largestCM': match.largestCM,
                    'has_tree': match.has_tree,
                    'tree_size': match.tree_size,
                    'tree_url': match.tree_url,
                }

            processed_mh_data.extend(batch_limit(
                session, MH_Match, filtered_ids['MH_Match'],
                process_mh_match, limit
            ))

        if mh_ancestors and filtered_ids.get('MH_Ancestors'):
            def process_mh_ancestors(ancestor):
                return {
                    'unique_id': generate_unique_id(ancestor.TreeId, ancestor.personId),
                    'sex': 1 if ancestor.gender == 'F' else 0 if ancestor.gender == 'M' else 2,
                    'color': 27,
                    'TreeId': ancestor.TreeId,
                    'matchid': ancestor.matchid,
                    'surname': ancestor.surname,
                    'given': ancestor.given,
                    'birthdate': ancestor.birthdate,
                    'deathdate': ancestor.deathdate,
                    'birthplace': ancestor.birthplace,
                    'deathplace': ancestor.deathplace,
                    'personId': ancestor.personId,
                    'fatherId': ancestor.fatherId,
                    'motherId': ancestor.motherId,
                }

            processed_mh_data.extend(batch_limit(
                session, MH_Ancestors, filtered_ids['MH_Ancestors'],
                process_mh_ancestors, limit
            ))

        if mh_chromo and filtered_ids.get('MH_Chromo'):
            def process_mh_chromo(chromo):
                return {
                    'unique_id': generate_unique_id(chromo.guid, chromo.chromosome, chromo.start),
                    'color': 27,
                    'guid': chromo.guid,
                    'guid1': chromo.guid1,
                    'guid2': chromo.guid2,
                    'chromosome': chromo.chromosome,
                    'cm': chromo.cm,
                    'start': chromo.start,
                    'end': chromo.end,
                    'snps': chromo.snps,
                }

            processed_mh_data.extend(batch_limit(
                session, MH_Chromo, filtered_ids['MH_Chromo'],
                process_mh_chromo, limit
            ))

        if mh_icw and filtered_ids.get('MH_ICW'):
            def process_mh_icw(icw):
                return {
                    'unique_id': generate_unique_id(icw.id1, icw.id2),
                    'color': 27,
                    'id1': icw.id1,
                    'id2': icw.id2,
                    'totalCM': icw.totalCM,
                    'percent_shared': icw.percent_shared,
                    'num_segments': icw.num_segments,
                    'triTotalCM': icw.triTotalCM,
                    'triSegments': icw.triSegments,
                }

            processed_mh_data.extend(batch_limit(
                session, MH_ICW, filtered_ids['MH_ICW'],
                process_mh_icw, limit
            ))

        if mh_tree and filtered_ids.get('MH_Tree'):
            def process_mh_tree(tree):
                return {
                    'unique_id': generate_unique_id(tree.treeurl),
                    'color': 27,
                    'treeurl': tree.treeurl,
                    'Date': tree.created_date,
                    'updated_date': tree.updated_date,
                }

            processed_mh_data.extend(batch_limit(
                session, MH_Tree, filtered_ids['MH_Tree'],
                process_mh_tree, limit
            ))

    except Exception as e:
        logging.error(f"Error processing MyHeritage data: {e}")

    return processed_mh_data


# Import data into RootsMagic PersonTable.
def insert_person(person_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_person')
    logging.info("Inserting or updating individuals in PersonTable...")

    if person_rm_session is None:
        logging.error("Invalid person_rm_session object provided")
        raise ValueError("A valid Session object must be provided")

    try:
        processed_count = 0
        blank_record_count = 0
        for data in processed_data:
            person_id = data.get('PersonID')
            relid = data.get('relid')
            unique_id = data.get('unique_id')
            sex_value = data.get('sex', '')

            # logging.debug(f"Processing record: PersonID: {person_id}, "
            #              f"UniqueID: {unique_id}, sex: {sex_value}, relid: {relid}")

            if person_id is None and not unique_id:
                blank_record_count += 1
                logging.warning(f"Potentially problematic record: PersonID: None, "
                                f"UniqueID: None, sex: {sex_value}, relid: {relid}")
                continue  # Skip processing if both PersonID and UniqueID are missing

            person_data = {
                'UniqueID': unique_id,
                'Sex': sex_value,
                'Color': data.get('color', ''),
                'UTCModDate': func.julianday('now') - 2415018.5,
            }

            if person_id is not None:
                person_data['PersonID'] = person_id

            existing_person = None
            if person_id is not None:
                existing_person = person_rm_session.query(PersonTable).filter_by(PersonID=person_id).first()
            if not existing_person and unique_id:
                existing_person = person_rm_session.query(PersonTable).filter_by(UniqueID=unique_id).first()

            if existing_person:
                if relid != '1':
                    for key, value in person_data.items():
                        setattr(existing_person, key, value)
                    # logging.debug(f"Updated existing record: PersonID: {person_id}, UniqueID: {unique_id}")
                else:
                    setattr(existing_person, 'UTCModDate', person_data['UTCModDate'])
                    # logging.debug(f"Updated existing record (relid=1): PersonID: {person_id}, UniqueID: {unique_id}")
            else:
                new_person = PersonTable(**person_data)
                person_rm_session.add(new_person)
                # logging.debug(f"Inserted new record: PersonID: {person_id}, UniqueID: {unique_id}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                person_rm_session.flush()

        person_rm_session.commit()
        logging.info(f"Processed {processed_count} person records. {blank_record_count} "
                     f"records had neither PersonID nor UniqueID.")

    except Exception as e:
        logging.error(f"Error inserting or updating PersonTable: {e}")
        person_rm_session.rollback()
        raise

    finally:
        person_rm_session.close()


# Import data into RootsMagic NameTable.
def insert_name(name_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_name')
    logging.info("Inserting or updating names in NameTable...")

    if name_rm_session is None:
        logging.error("Invalid name_rm_session object provided")
        raise ValueError("A valid Session object must be provided")

    try:
        processed_count = 0
        for data in processed_data:
            # Get PersonID from processed_data
            person_id = data.get('PersonID') or data.get('personId')

            # If person_id is None, try to find it in the PersonTable
            if person_id is None:
                unique_id = data.get('unique_id')
                if unique_id:
                    person_record = name_rm_session.query(PersonTable).filter_by(UniqueID=unique_id).first()
                    if person_record:
                        person_id = person_record.PersonID
                    else:
                        logging.warning(f"No matching PersonID found for UniqueID: {unique_id}")
                else:
                    logging.warning("No PersonID or UniqueID available for this record")

            # Map processed_data to NameTable columns
            name_data = {
                'OwnerID': person_id,
                'Surname': data.get('Surname', ''),
                'Given': data.get('Given', ''),
                'NameType': data.get('NameType', ''),
                'IsPrimary': data.get('IsPrimary', ''),
                'SurnameMP': data.get('Surname', ''),
                'GivenMP': data.get('Given', ''),
                'UTCModDate': func.julianday('now') - 2415018.5,
            }

            # Check if a name record already exists for this person and name type
            existing_name = None
            if person_id is not None:
                existing_name = name_rm_session.query(NameTable).filter_by(OwnerID=person_id).first()

            if existing_name:
                # Update existing record
                for key, value in name_data.items():
                    setattr(existing_name, key, value)
                # logging.debug(f"Updated name record for OwnerID: {person_id} {name_data}")
            else:
                # Create new record
                new_name = NameTable(**name_data)
                name_rm_session.add(new_name)
                # logging.debug(f"Inserted new name record for OwnerID: {person_id} {name_data}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                name_rm_session.flush()

        name_rm_session.commit()
        logging.info(f"Processed {processed_count} name records.")

    except Exception as e:
        logging.error(f"Error inserting or updating NameTable: {e}")
        name_rm_session.rollback()
        raise

    finally:
        name_rm_session.close()


# Import data into RootsMagic FamilyTable
def insert_family(family_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_family')
    logging.info("Inserting or updating family data in FamilyTable...")

    try:
        processed_count = 0

        for data in processed_data:
            family_id = data.get('FamilyID')
            father_id = data.get('FatherID')
            mother_id = data.get('MotherID')
            child_id = data.get('PersonID')

            # logging.debug(f"Processing family record: FamilyID: {family_id}, "
            #               f"FatherID: {father_id}, MotherID: {mother_id}, ChildID: {child_id}")

            # If FamilyID is provided, look for an existing record with that FamilyID
            if family_id:
                existing_family = family_rm_session.query(FamilyTable).filter_by(FamilyID=family_id).first()
            else:
                # If no FamilyID, look for an existing record with matching FatherID, MotherID, and ChildID
                existing_family = family_rm_session.query(FamilyTable).filter(
                    FamilyTable.FatherID == father_id,
                    FamilyTable.MotherID == mother_id,
                    FamilyTable.ChildID == child_id
                ).first()

            if existing_family:
                # Merge new data into the existing record
                for key, value in data.items():
                    if key in FamilyTable.__table__.columns and value is not None:
                        setattr(existing_family, key, value)
                existing_family.UTCModDate = func.julianday('now') - 2415018.5
                data['FamilyID'] = existing_family.FamilyID  # Update FamilyID in data
                # logging.debug(f"Updated existing family record: FamilyID: "
                #               f"{existing_family.FamilyID}, ChildID: {child_id}")
            else:
                # Create new record in FamilyTable
                family_data = {
                    'FatherID': father_id,
                    'MotherID': mother_id,
                    'ChildID': child_id,
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_family = FamilyTable(**family_data)
                family_rm_session.add(new_family)
                family_rm_session.flush()  # Ensure the new record is written to the database

                # Retrieve the FamilyID of the new record
                data['FamilyID'] = new_family.FamilyID
                # logging.debug(f"Inserted new family record: FamilyID: {data['FamilyID']}, ChildID: {child_id}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                family_rm_session.flush()

        family_rm_session.commit()
        logging.info(f"Processed {processed_count} family records.")
        return processed_data

    except Exception as e:
        logging.error(f"Error inserting or updating FamilyTable: {e}")
        family_rm_session.rollback()
        raise
    finally:
        family_rm_session.close()


# Import data into RootsMagic ChildTable
def insert_child(child_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_child')
    logging.info("Inserting or updating children in ChildTable...")

    try:
        processed_count = 0

        for data in processed_data:
            child_id = data.get('PersonID')
            family_id = data.get('FamilyID')

            # Check if both ChildID and FamilyID are present
            if not child_id or not family_id:
                logging.warning(f"Skipped processing record due to missing ChildID or FamilyID: "
                                f"ChildID={child_id}, FamilyID={family_id}")
                continue

            # Check if a child record already exists for the given ChildID and FamilyID
            existing_child = child_rm_session.query(ChildTable).filter_by(
                ChildID=child_id, FamilyID=family_id).first()

            if existing_child:
                # Merge new data into the existing record
                for key, value in data.items():
                    if key in ChildTable.__table__.columns and value is not None:
                        setattr(existing_child, key, value)
                existing_child.UTCModDate = func.julianday('now') - 2415018.5
                # logging.debug(
                #     f"Updated existing child record for ChildID: {child_id} and FamilyID: {family_id}")
            else:
                # Create new record in ChildTable
                child_data = {
                    'ChildID': child_id,
                    'FamilyID': family_id,
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_child = ChildTable(**child_data)
                child_rm_session.add(new_child)
                # logging.debug(f"Inserted new child record for ChildID: {child_id} and FamilyID: {family_id}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                child_rm_session.flush()

        child_rm_session.commit()
        logging.info(f"Processed {processed_count} child records.")

    except Exception as e:
        logging.error(f"Error inserting or updating ChildTable: {e}")
        child_rm_session.rollback()
        raise
    finally:
        child_rm_session.close()


# Import data into RootsMagic DNATable
def insert_dna(dna_rm_session: Session, processed_data, selected_kits, batch_size=limit):
    logging.getLogger('insert_dna')
    logging.info("Inserting or updating DNA data in DNATable...")

    try:
        processed_count = 0

        for kit in selected_kits:
            selected_kit_guid = kit[1]
            logging.info(f"Processing data for kit GUID: {selected_kit_guid}")

            for data in processed_data:

                # Check for the specific marker that identifies data from process_matchgroup
                if 'source' in data and data['source'] == 'process_matchgroup':
                    # logging.debug("Data is identified as from process_matchgroup")

                    person_1 = dna_rm_session.query(PersonTable).filter_by(UniqueID=selected_kit_guid).first()
                    person_id_1 = person_1.PersonID if person_1 else None

                    person_id_2 = data.get('PersonID')

                    label1 = f"{data['Given']} {data['Surname']}"
                    label2 = data['unique_id']
                    note = (f"https://www.ancestry.com/discoveryui-matches/compare/"
                            f"{selected_kit_guid}/with/{data['unique_id']}")
                    shared_cm = data.get('sharedCM')
                    date = data.get('Date') or data.get('matchRunDate')

                    # Construct dna_data for insertion
                    dna_data = {
                        'ID1': person_id_1,
                        'ID2': person_id_2,
                        'Label1': label1,
                        'Label2': label2,
                        'DNAProvider': data.get('DNAProvider'),
                        'SharedCM': shared_cm,
                        'SharedPercent': round(shared_cm / 69, 2) if shared_cm else None,
                        'SharedSegs': data.get('SharedSegs'),
                        'Date': date,
                        'Note': note,
                        'UTCModDate': func.julianday('now') - 2415018.5,
                    }

                    # logging.debug(f"DNA data constructed for insertion: {dna_data}")

                    # Check if a DNA record already exists for the given ID1 and ID2
                    existing_dna = dna_rm_session.query(DNATable).filter_by(ID1=person_id_1, ID2=person_id_2).first()

                    if existing_dna:
                        # Update existing record
                        for key, value in dna_data.items():
                            setattr(existing_dna, key, value)
                        existing_dna.UTCModDate = func.julianday('now') - 2415018.5
                        # logging.debug(f"Updated existing DNA record for ID1: {person_id_1} and ID2: {person_id_2}")
                    else:
                        # Create new record in DNATable
                        new_dna = DNATable(**dna_data)
                        dna_rm_session.add(new_dna)
                        # logging.debug(f"Inserted new DNA record for ID1: {person_id_1} and ID2: {person_id_2}")

                    processed_count += 1
                    if batch_size > 0 and processed_count % batch_size == 0:
                        dna_rm_session.flush()

        dna_rm_session.commit()
        logging.info(f"Processed {processed_count} DNA records.")

    except Exception as e:
        logging.error(f"Error inserting or updating DNATable: {e}")
        dna_rm_session.rollback()
        raise
    finally:
        dna_rm_session.close()


# Import data into RootsMagic EventTable
def insert_event(event_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_event')
    logging.info("Inserting or updating events in EventTable...")

    try:
        processed_count = 0

        for data in processed_data:
            try:
                # Ensure 'OwnerID' and 'EventType' exist in data
                if 'OwnerID' not in data:
                    logging.warning(f"Missing required keys in data: {data}")
                    continue  # Skip processing this data item

                # Check if an event record already exists for the given OwnerID and EventType
                existing_event = event_rm_session.query(EventTable).filter_by(
                    OwnerID=data['OwnerID'], EventType=data['EventType']).first()

                if existing_event:
                    # Update existing record
                    for key, value in data.items():
                        setattr(existing_event, key, value)
                    existing_event.UTCModDate = func.julianday('now') - 2415018.5
                    logging.info(
                        f"Updated existing event record for OwnerID: {data['OwnerID']} "
                        f"and EventType: {data['EventType']}")
                else:
                    # Create new record in EventTable
                    event_data = {
                        'EventType': data.get('EventType', None),
                        'OwnerType': 1,
                        'OwnerID': data.get('match_trees', {}).get('personId', None),
                        'FamilyID': data.get('FamilyID', None),
                        'PlaceID': data.get('PlaceID', None),
                        'SiteID': data.get('SiteID', None),
                        'Date': data.get('Date', None),
                        'SortDate': data.get('SortDate', None),
                        'IsPrimary': data.get('IsPrimary', None),
                        'IsPrivate': data.get('IsPrivate', None),
                        'Proof': data.get('Proof', None),
                        'Status': data.get('Status', None),
                        'Sentence': data.get('Sentence', None),
                        'Details': data.get('Details', None),
                        'Note': data.get('Note', None),
                        'UTCModDate': func.julianday('now') - 2415018.5,
                    }
                    new_event = EventTable(**event_data)
                    event_rm_session.add(new_event)
                    logging.info(
                        f"Inserted new event record for OwnerID: {data['OwnerID']} and EventType: {data['EventType']}")

                processed_count += 1
                if batch_size > 0 and processed_count % batch_size == 0:
                    event_rm_session.flush()

            except Exception as inner_e:
                logging.error(f"Error processing data {data}: {inner_e}")

        event_rm_session.commit()
        logging.info(f"Processed {processed_count} event records.")

    except Exception as e:
        logging.error(f"Error inserting or updating EventTable: {e}")
        event_rm_session.rollback()
        raise
    finally:
        event_rm_session.close()


# Import data into RootsMagic PlaceTable
def insert_place(place_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_place')
    logging.info("Inserting or updating places in PlaceTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a place record already exists for the given Name
            existing_place = place_rm_session.query(PlaceTable).filter_by(Name=data['Name']).first()

            if existing_place:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_place, key, value)
                existing_place.UTCModDate = func.julianday('now') - 2415018.5
                logging.info(f"Updated existing place record for Name: {data['Name']}")
            else:
                # Create new record in PlaceTable
                place_data = {
                    'PlaceType': data.get('PlaceType', None),
                    'Name': data.get('Name', None),
                    'Abbrev': data.get('Abbrev', None),
                    'Normalized': data.get('Normalized', None),
                    'Latitude': data.get('Latitude', None),
                    'Longitude': data.get('Longitude', None),
                    'LatLongExact': data.get('LatLongExact', None),
                    'MasterID': data.get('MasterID', None),
                    'Note': data.get('Note', None),
                    'Reverse': data.get('Reverse', None),
                    'fsID': data.get('fsID', None),
                    'anID': data.get('anID', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_place = PlaceTable(**place_data)
                place_rm_session.add(new_place)
                logging.info(f"Inserted new place record for Name: {data['Name']}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                place_rm_session.flush()

        place_rm_session.commit()
        logging.info(f"Processed {processed_count} place records.")

    except Exception as e:
        logging.error(f"Error inserting or updating PlaceTable: {e}")
        place_rm_session.rollback()
        raise
    finally:
        place_rm_session.close()


# Import data into RootsMagic GroupTable
def insert_group(group_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_group')
    logging.info("Inserting or updating group data in GroupTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a group record already exists for the given GroupID
            existing_group = group_rm_session.query(GroupTable).filter_by(
                GroupID=data['GroupID']).first()

            if existing_group:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_group, key, value)
                existing_group.UTCModDate = func.julianday('now') - 2415018.5
                logging.info(f"Updated existing group record for GroupID: {data['GroupID']}")
            else:
                # Create new record in GroupTable
                group_data = {
                    'GroupID': data.get('GroupID', None),
                    'StartID': data.get('StartID', None),
                    'EndID': data.get('EndID', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_group = GroupTable(**group_data)
                group_rm_session.add(new_group)
                logging.info(f"Inserted new group record for GroupID: {data['GroupID']}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                group_rm_session.flush()

        group_rm_session.commit()
        logging.info(f"Processed {processed_count} group records.")

    except Exception as e:
        logging.error(f"Error inserting or updating GroupTable: {e}")
        group_rm_session.rollback()
        raise
    finally:
        group_rm_session.close()


# Import data into RootsMagic URLTable
def insert_url(url_rm_session: Session, processed_data, batch_size=limit):
    logging.getLogger('insert_url')
    logging.info("Inserting or updating URL data in URLTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a URL record already exists for the given OwnerType and OwnerID
            existing_url = url_rm_session.query(URLTable).filter_by(
                OwnerType=data['OwnerType'], OwnerID=data['OwnerID']).first()

            if existing_url:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_url, key, value)
                existing_url.UTCModDate = func.julianday('now') - 2415018.5
                logging.info(
                    f"Updated existing URL record for OwnerType {data['OwnerType']} and OwnerID {data['OwnerID']}")
            else:
                # Create new record in URLTable
                url_data = {
                    'OwnerType': data.get('OwnerType', None),
                    'OwnerID': data.get('OwnerID', None),
                    'LinkType': data.get('LinkType', None),
                    'Name': data.get('Name', None),
                    'URL': data.get('URL', None),
                    'Note': data.get('Note', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_url = URLTable(**url_data)
                url_rm_session.add(new_url)
                logging.info(f"Inserted new URL record for OwnerType {data['OwnerType']} and OwnerID {data['OwnerID']}")

            processed_count += 1
            if batch_size > 0 and processed_count % batch_size == 0:
                url_rm_session.flush()

        url_rm_session.commit()
        logging.info(f"Processed {processed_count} URL records.")

    except Exception as e:
        logging.error(f"Error inserting or updating URLTable: {e}")
        url_rm_session.rollback()
        raise
    finally:
        url_rm_session.close()


def main():
    setup_logging()
    logging.info("Connecting to databases...")
    dnagedcom_db_path, rootsmagic_db_path = find_database_paths()

    # Connect to DNAGedcom and RootsMagic databases using SQLAlchemy
    dg_session, dg_engine, rm_session, rm_engine = connect_to_db_sqlalchemy(dnagedcom_db_path, rootsmagic_db_path)

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

        filtered_ids = filter_selected_kits(dg_session, selected_kits)
        import_profiles(rm_session, selected_kits)

        # Process different providers and combine into processed_data.
        processed_ancestry_data = process_ancestry(dg_session, filtered_ids)
        processed_ftdna_data = process_ftdna(dg_session, filtered_ids)
        processed_mh_data = process_mh(dg_session, filtered_ids)
        processed_data = processed_ancestry_data + processed_ftdna_data + processed_mh_data

        try:
            insert_fact_type(rm_session)
            insert_person(rm_session, processed_data)
            insert_name(rm_session, processed_data)
            insert_family(rm_session, processed_data)
            insert_child(rm_session, processed_data)
            insert_dna(rm_session, processed_data, selected_kits)
            # insert_place(rm_session, processed_data)
            # insert_event(rm_session, processed_data)
            # insert_url(rm_session, processed_data)
            # insert_group(rm_session, processed_data)
        except Exception as e:
            logging.error(f"Error during data insertion: {e}")
    else:
        logging.warning("No kits found.")

    # Close sessions and engines
    dg_session.close()
    dg_engine.dispose()
    rm_session.close()
    rm_engine.dispose()


if __name__ == "__main__":
    main()
