# import_data.py
# Local Imports
from sqlalchemy import func

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
limit = 500
# Ancestry
ancestry_matchgroups = 1
ancestry_matchtrees = 1
ancestry_treedata = 0
ancestry_icw = 0
ancestry_ancestorcouple = 0
ancestry_matchethnicity = 0
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
    global ancestry_matchgroups, ancestry_matchtrees, ancestry_treedata, ancestry_icw, \
        ancestry_ancestorcouple, ancestry_matchethnicity
    global ftdna_matches2, ftdna_chromo2, ftdna_icw2, dg_tree, dg_individual
    global mh_match, mh_ancestors, mh_chromo, mh_icw, mh_tree
    logger = logging.getLogger('filter_selected_kits')
    logger.info("Filtering selected kits...")

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
                name = group.matchTestDisplayName
                given, surname = (name.split()[0], name.split()[-1]) if len(name.split()) > 1 else (name, "")
                return {
                    'unique_id': group.matchGuid,
                    'sex': 1 if group.subjectGender == 'F' else 0 if group.subjectGender == 'M' else 2,
                    'color': 25,
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

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_matchGroups, filtered_ids['Ancestry_matchGroups'],
                process_matchgroup, limit
            ))

        if ancestry_matchtrees and filtered_ids.get('Ancestry_matchTrees'):
            def process_matchtree(tree):
                sex = 2
                if tree.personId:
                    if tree.fatherId and tree.personId in tree.fatherId:
                        sex = 0
                    elif tree.motherId and tree.personId in tree.motherId:
                        sex = 1

                return {
                    'unique_id': generate_unique_id(tree.given, tree.surname, tree.birthdate),
                    'sex': sex,
                    'color': 24,
                    'matchid': tree.matchid,
                    'Surname': tree.surname,
                    'Given': tree.given,
                    'birthdate': tree.birthdate,
                    'deathdate': tree.deathdate,
                    'birthplace': tree.birthplace,
                    'deathplace': tree.deathplace,
                    'relid': tree.relid,
                    'personId': tree.personId,
                    'fatherId': tree.fatherId,
                    'motherId': tree.motherId,
                    'DNAProvider': 2,
                    'created_date': tree.created_date,
                    'IsPrimary': 1,
                    'NameType': 2,
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_matchTrees, filtered_ids['Ancestry_matchTrees'],
                process_matchtree, limit
            ))

        if ancestry_treedata and filtered_ids.get('Ancestry_TreeData'):
            def process_treedata(treedata):
                return {
                    'TestGuid': treedata.TestGuid,
                    'TreeSize': treedata.TreeSize,
                    'PublicTree': treedata.PublicTree,
                    'PrivateTree': treedata.PrivateTree,
                    'UnlinkedTree': treedata.UnlinkedTree,
                    'TreeId': treedata.TreeId,
                    'NoTrees': treedata.NoTrees,
                    'TreeUnavailable': treedata.TreeUnavailable,
                    'matchGuid': treedata.matchGuid,
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_TreeData, filtered_ids['Ancestry_TreeData'],
                process_treedata, limit
            ))

        if ancestry_icw and filtered_ids.get('Ancestry_ICW'):
            def process_icw(icw):
                return {
                    'matchid': icw.matchid,
                    'icwid': icw.icwid,
                    'created_date': icw.created_date,
                    'sharedCM': icw.sharedCentimorgans,
                    'confidence': icw.confidence,
                    'meiosis': icw.meiosis,
                    'numSharedSegments': icw.numSharedSegments,
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_ICW, filtered_ids['Ancestry_ICW'],
                process_icw, limit
            ))

        if ancestry_matchethnicity and filtered_ids.get('Ancestry_matchEthnicity'):
            def process_matchethnicity(ethnicity):
                return {
                    'unique_id': generate_unique_id(ethnicity.matchGuid),
                    'matchGuid': ethnicity.matchGuid,
                    'ethnicregions': ethnicity.ethnicregions,
                    'ethnictraceregions': ethnicity.ethnictraceregions,
                    'created_date': ethnicity.created_date,
                    'percent': ethnicity.percent,
                    'version': ethnicity.version,
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, Ancestry_matchEthnicity, filtered_ids['Ancestry_matchEthnicity'],
                process_matchethnicity, limit
            ))

        if ancestry_ancestorcouple and filtered_ids.get('AncestryAncestorCouple'):
            def process_ancestorcouple(couple):
                return {
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
                    'MotherRelationshipFromSampleToMatch': couple.MotherRelationshipFromSampleToMatch
                }

            processed_ancestry_data.extend(process_table_with_limit(
                session, AncestryAncestorCouple, filtered_ids['AncestryAncestorCouple'],
                process_ancestorcouple, limit
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

            processed_ftdna_data.extend(process_table_with_limit(
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

            processed_ftdna_data.extend(process_table_with_limit(
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

            processed_ftdna_data.extend(process_table_with_limit(
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

            processed_ftdna_data.extend(process_table_with_limit(
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

            processed_ftdna_data.extend(process_table_with_limit(
                session, DGIndividual, filtered_ids['DGIndividual'],
                process_dg_individual, limit
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

            processed_mh_data.extend(process_table_with_limit(
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

            processed_mh_data.extend(process_table_with_limit(
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

            processed_mh_data.extend(process_table_with_limit(
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

            processed_mh_data.extend(process_table_with_limit(
                session, MH_ICW, filtered_ids['MH_ICW'],
                process_mh_icw, limit
            ))

        if mh_tree and filtered_ids.get('MH_Tree'):
            def process_mh_tree(tree):
                return {
                    'unique_id': generate_unique_id(tree.treeurl),
                    'color': 27,
                    'treeurl': tree.treeurl,
                    'created_date': tree.created_date,
                    'updated_date': tree.updated_date,
                }

            processed_mh_data.extend(process_table_with_limit(
                session, MH_Tree, filtered_ids['MH_Tree'],
                process_mh_tree, limit
            ))

    except Exception as e:
        logger.error(f"Error processing MyHeritage data: {e}")

    return processed_mh_data


def insert_fact_type(fact_rm_session: Session):
    logger = logging.getLogger('insert_fact_type')

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
            logger.info("Fact Type 'DNA Kit' inserted into FactTypeTable.")

        else:
            # Update existing Fact Type 'DNA Kit'
            fact_type.OwnerType = 0
            fact_type.Name = 'DNA Kit'
            fact_type.Abbrev = 'DNA Kit'
            fact_type.GedcomTag = 'EVEN'
            fact_type.UseValue = 1
            fact_type.UseDate = 0
            fact_type.UsePlace = 0
            fact_type.Sentence = '[person] had a DNA test performed. View DNA Tab in profile to view matches.'
            fact_type.Flags = 2147483647
            fact_type.UTCModDate = func.julianday('now') - 2415018.5
            fact_rm_session.commit()
            logger.info("Fact Type 'DNA Kit' updated in FactTypeTable.")

    except Exception as e:
        logger.error(f"Error inserting or updating Fact Type 'DNA Kit' in FactTypeTable: {e}")
        fact_rm_session.rollback()
        raise
    finally:
        fact_rm_session.close()


def insert_person(person_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_person')
    logger.info("Inserting or updating individuals in PersonTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if the person with UniqueID already exists in the database
            person = person_rm_session.query(PersonTable).filter_by(UniqueID=data['unique_id']).first()

            if not person:
                # Create a new person if it doesn't exist
                person = PersonTable(
                    UniqueID=data['unique_id'],
                    Sex=data['sex'],
                    Color=data['color'],
                    UTCModDate=func.julianday('now') - 2415018.5
                )
                person_rm_session.add(person)
                person_rm_session.flush()  # Ensure the person is written to get the ID
            else:
                # Update existing person's attributes
                person.Sex = data['sex']
                person.Color = data['color']
                person.UTCModDate = func.julianday('now') - 2415018.5

            # Retrieve and assign the generated PersonID
            data['PersonID'] = person.PersonID

            processed_count += 1
            if processed_count % batch_size == 0:
                person_rm_session.flush()

        person_rm_session.commit()
        logger.info(f"Processed {processed_count} individuals.")
    except Exception as e:
        logger.error(f"Error inserting or updating PersonTable: {e}")
        person_rm_session.rollback()
        raise  # Re-raise the exception to handle it elsewhere if needed
    finally:
        person_rm_session.close()


def insert_name(name_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_name')
    logger.info("Inserting or updating names in NameTable...")

    if name_rm_session is None:
        logger.error("Invalid name_rm_session object provided")
        raise ValueError("A valid Session object must be provided")

    try:
        processed_count = 0
        for data in processed_data:
            # Get PersonID from processed_data
            person_id = data.get('PersonID')
            if not person_id:
                logger.warning(f"PersonID not found for data: {data}")
                continue

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
            existing_name = name_rm_session.query(NameTable).filter_by(OwnerID=name_data['OwnerID'], NameType=2).first()

            if existing_name:
                # Update existing record
                for key, value in name_data.items():
                    setattr(existing_name, key, value)
            else:
                # Create new record
                new_name = NameTable(**name_data)
                name_rm_session.add(new_name)

            processed_count += 1
            if processed_count % batch_size == 0:
                name_rm_session.flush()

        name_rm_session.commit()
        logger.info(f"Processed {processed_count} name records.")

    except Exception as e:
        logger.error(f"Error inserting or updating NameTable: {e}")
        name_rm_session.rollback()
        raise

    finally:
        name_rm_session.close()


def insert_dna(dna_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_dna')
    logger.info("Inserting or updating DNA data in DNATable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a DNA record already exists for the given ID1 and ID2
            existing_dna = dna_rm_session.query(DNATable).filter_by(ID1=data['ID1'], ID2=data['ID2']).first()

            if existing_dna:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_dna, key, value)
                existing_dna.UTCModDate = func.julianday('now') - 2415018.5
                logger.info(f"Updated existing DNA record for ID1: {data['ID1']} and ID2: {data['ID2']}")
            else:
                # Create new record in DNATable
                dna_data = {
                    'ID1': data.get('ID1', None),
                    'ID2': data.get('ID2', None),
                    'Label1': data.get('Label1', None),
                    'Label2': data.get('Label2', None),
                    'DNAProvider': data.get('DNAProvider', None),
                    'SharedCM': data.get('SharedCM', None),
                    'SharedPercent': data.get('SharedPercent', None),
                    'LargeSeg': data.get('LargeSeg', None),
                    'SharedSegs': data.get('SharedSegs', None),
                    'Date': data.get('Date', None),
                    'Relate1': data.get('Relate1', None),
                    'Relate2': data.get('Relate2', None),
                    'CommonAnc': data.get('CommonAnc', None),
                    'CommonAncType': data.get('CommonAncType', None),
                    'Verified': data.get('Verified', None),
                    'Note': data.get('Note', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_dna = DNATable(**dna_data)
                dna_rm_session.add(new_dna)
                logger.info(f"Inserted new DNA record for ID1: {data['ID1']} and ID2: {data['ID2']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                dna_rm_session.flush()

        dna_rm_session.commit()
        logger.info(f"Processed {processed_count} DNA records.")

    except Exception as e:
        logger.error(f"Error inserting or updating DNATable: {e}")
        dna_rm_session.rollback()
        raise
    finally:
        dna_rm_session.close()


def insert_event(event_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_event')
    logger.info("Inserting or updating events in EventTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if an event record already exists for the given OwnerID and EventType
            existing_event = event_rm_session.query(EventTable).filter_by(
                OwnerID=data['OwnerID'], EventType=data['EventType']).first()

            if existing_event:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_event, key, value)
                existing_event.UTCModDate = func.julianday('now') - 2415018.5
                logger.info(
                    f"Updated existing event record for OwnerID: {data['OwnerID']} and EventType: {data['EventType']}")
            else:
                # Create new record in EventTable
                event_data = {
                    'EventType': data.get('EventType', None),
                    'OwnerType': 1,
                    'OwnerID': data.get('OwnerID', None),
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
                logger.info(
                    f"Inserted new event record for OwnerID: {data['OwnerID']} and EventType: {data['EventType']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                event_rm_session.flush()

        event_rm_session.commit()
        logger.info(f"Processed {processed_count} event records.")

    except Exception as e:
        logger.error(f"Error inserting or updating EventTable: {e}")
        event_rm_session.rollback()
        raise
    finally:
        event_rm_session.close()


def insert_place(place_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_place')
    logger.info("Inserting or updating places in PlaceTable...")

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
                logger.info(f"Updated existing place record for Name: {data['Name']}")
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
                logger.info(f"Inserted new place record for Name: {data['Name']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                place_rm_session.flush()

        place_rm_session.commit()
        logger.info(f"Processed {processed_count} place records.")

    except Exception as e:
        logger.error(f"Error inserting or updating PlaceTable: {e}")
        place_rm_session.rollback()
        raise
    finally:
        place_rm_session.close()


def insert_child(child_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_child')
    logger.info("Inserting or updating children in ChildTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a child record already exists for the given ChildID and FamilyID
            existing_child = child_rm_session.query(ChildTable).filter_by(
                ChildID=data['ChildID'], FamilyID=data['FamilyID']).first()

            if existing_child:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_child, key, value)
                existing_child.UTCModDate = func.julianday('now') - 2415018.5
                logger.info(
                    f"Updated existing child record for ChildID: {data['ChildID']} and FamilyID: {data['FamilyID']}")
            else:
                # Create new record in ChildTable
                child_data = {
                    'ChildID': data.get('ChildID', None),
                    'FamilyID': data.get('FamilyID', None),
                    'RelFather': data.get('RelFather', None),
                    'RelMother': data.get('RelMother', None),
                    'ChildOrder': data.get('ChildOrder', None),
                    'IsPrivate': data.get('IsPrivate', None),
                    'ProofFather': data.get('ProofFather', None),
                    'ProofMother': data.get('ProofMother', None),
                    'Note': data.get('Note', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_child = ChildTable(**child_data)
                child_rm_session.add(new_child)
                logger.info(
                    f"Inserted new child record for ChildID: {data['ChildID']} and FamilyID: {data['FamilyID']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                child_rm_session.flush()

        child_rm_session.commit()
        logger.info(f"Processed {processed_count} child records.")

    except Exception as e:
        logger.error(f"Error inserting or updating ChildTable: {e}")
        child_rm_session.rollback()
        raise
    finally:
        child_rm_session.close()


def insert_family(family_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_family')
    logger.info("Inserting or updating family data in FamilyTable...")

    try:
        processed_count = 0

        for data in processed_data:
            # Check if a family record already exists for the given FamilyID
            existing_family = family_rm_session.query(FamilyTable).filter_by(
                FamilyID=data['FamilyID']).first()

            if existing_family:
                # Update existing record
                for key, value in data.items():
                    setattr(existing_family, key, value)
                existing_family.UTCModDate = func.julianday('now') - 2415018.5
                logger.info(f"Updated existing family record for FamilyID: {data['FamilyID']}")
            else:
                # Create new record in FamilyTable
                family_data = {
                    'FamilyID': data.get('FamilyID', None),
                    'FatherID': data.get('FatherID', None),
                    'MotherID': data.get('MotherID', None),
                    'ChildID': data.get('ChildID', None),
                    'HusbOrder': data.get('HusbOrder', None),
                    'WifeOrder': data.get('WifeOrder', None),
                    'IsPrivate': data.get('IsPrivate', None),
                    'Proof': data.get('Proof', None),
                    'SpouseLabel': data.get('SpouseLabel', None),
                    'FatherLabel': data.get('FatherLabel', None),
                    'MotherLabel': data.get('MotherLabel', None),
                    'SpouseLabelStr': data.get('SpouseLabelStr', None),
                    'FatherLabelStr': data.get('FatherLabelStr', None),
                    'MotherLabelStr': data.get('MotherLabelStr', None),
                    'Note': data.get('Note', None),
                    'UTCModDate': func.julianday('now') - 2415018.5,
                }
                new_family = FamilyTable(**family_data)
                family_rm_session.add(new_family)
                logger.info(f"Inserted new family record for FamilyID: {data['FamilyID']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                family_rm_session.flush()

        family_rm_session.commit()
        logger.info(f"Processed {processed_count} family records.")

    except Exception as e:
        logger.error(f"Error inserting or updating FamilyTable: {e}")
        family_rm_session.rollback()
        raise
    finally:
        family_rm_session.close()


def insert_group(group_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_group')
    logger.info("Inserting or updating group data in GroupTable...")

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
                logger.info(f"Updated existing group record for GroupID: {data['GroupID']}")
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
                logger.info(f"Inserted new group record for GroupID: {data['GroupID']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                group_rm_session.flush()

        group_rm_session.commit()
        logger.info(f"Processed {processed_count} group records.")

    except Exception as e:
        logger.error(f"Error inserting or updating GroupTable: {e}")
        group_rm_session.rollback()
        raise
    finally:
        group_rm_session.close()


def insert_url(url_rm_session: Session, processed_data, batch_size=1000):
    logger = logging.getLogger('insert_url')
    logger.info("Inserting or updating URL data in URLTable...")

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
                logger.info(
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
                logger.info(f"Inserted new URL record for OwnerType {data['OwnerType']} and OwnerID {data['OwnerID']}")

            processed_count += 1
            if processed_count % batch_size == 0:
                url_rm_session.flush()

        url_rm_session.commit()
        logger.info(f"Processed {processed_count} URL records.")

    except Exception as e:
        logger.error(f"Error inserting or updating URLTable: {e}")
        url_rm_session.rollback()
        raise
    finally:
        url_rm_session.close()


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

        filtered_ids = filter_selected_kits(dg_session, selected_kits)

        # Process different providers and combine into processed_data.
        processed_ancestry_data = process_ancestry(dg_session, filtered_ids)
        processed_ftdna_data = process_ftdna(dg_session, filtered_ids)
        processed_mh_data = process_mh(dg_session, filtered_ids)
        processed_data = processed_ancestry_data + processed_ftdna_data + processed_mh_data

        try:
            insert_fact_type(rm_session)
            insert_person(rm_session, processed_data)
            insert_name(rm_session, processed_data)
            # insert_event(rm_session, processed_data)
            # insert_child(rm_session, processed_data)
            # insert_family(rm_session, processed_data)
            # insert_dna(rm_session, processed_data)
            # insert_place(rm_session, processed_data)
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
