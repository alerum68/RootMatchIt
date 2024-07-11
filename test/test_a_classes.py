import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from classes.a_classes import Ancestry_Base, Ancestry_ICW, Ancestry_Ethnicity, Ancestry_matchEthnicity, \
    Ancestry_matchGroups, Ancestry_matchTrees, Ancestry_Profiles, Ancestry_TreeData


# Set up the SQLite in-memory database for testing
@pytest.fixture(scope='module')
def engine():
    return create_engine('sqlite:///:memory:')


@pytest.fixture(scope='module')
def tables(engine):
    # Create tables in the in-memory database
    Ancestry_Base.metadata.create_all(engine)
    yield
    # Drop tables after the test
    Ancestry_Base.metadata.drop_all(engine)


@pytest.fixture(scope='module')
def session(engine, tables):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_class_definitions():
    # Check if classes are defined and part of Ancestry_Base metadata
    assert 'Ancestry_ICW' in Ancestry_Base.metadata.tables
    assert 'Ancestry_Ethnicity' in Ancestry_Base.metadata.tables
    assert 'Ancestry_matchEthnicity' in Ancestry_Base.metadata.tables
    assert 'Ancestry_matchGroups' in Ancestry_Base.metadata.tables
    assert 'Ancestry_matchTrees' in Ancestry_Base.metadata.tables
    assert 'Ancestry_Profiles' in Ancestry_Base.metadata.tables
    assert 'Ancestry_TreeData' in Ancestry_Base.metadata.tables


def test_ancestry_icw_columns(session):
    # Check columns in Ancestry_ICW table
    instance = Ancestry_ICW(matchid='M1', matchname='Test Match', matchadmin='Admin', icwid='I1', icwname='ICW Test',
                            icwadmin='ICW Admin', source='Ancestry', created_date='2023-01-01',
                            loginUsername='user1', sync='Yes', GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ancestry_ethnicity_columns(session):
    # Check columns in Ancestry_Ethnicity table
    instance = Ancestry_Ethnicity(code='ETH1', value='European')
    session.add(instance)
    session.commit()
    assert instance.code is not None


def test_ancestry_matchethnicity_columns(session):
    # Check columns in Ancestry_matchEthnicity table
    instance = Ancestry_matchEthnicity(matchGuid='M1', ethnicregions='Europe', ethnictraceregions='Northern Europe',
                                       created_date='2023-01-01', percent=75, version=1)
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ancestry_matchgroups_columns(session):
    # Check columns in Ancestry_matchGroups table
    instance = Ancestry_matchGroups(testGuid='T1', matchGuid='M1', matchTestDisplayName='Test Display',
                                    matchTestAdminDisplayName='Test Admin', matchTreeNodeCount=1, groupName='Group1',
                                    confidence=0.95, sharedCentimorgans=100.5, fetchedSegmentInfo=1,
                                    totalSharedCentimorgans=150.5, longestSegmentCentimorgans=75.5,
                                    sharedSegment=1, lastLoggedInDate='2023-01-01', starred='Yes', viewed='Yes',
                                    matchTreeIsPrivate='No', hasHint='Yes', note='Test note', userPhoto='photo.jpg',
                                    matchTreeId='Tree1', treeId='Tree1', matchMemberSinceYear='2020',
                                    created_date='2023-01-01', icwRunDate='2023-01-02', treeRunDate='2023-01-03',
                                    matchRunDate='2023-01-04', loginUsername='user1', sync='Yes',
                                    matchTestAdminUcdmId='AdminUcdm', GF_Sync='Yes', paternal=1, maternal=1,
                                    subjectGender='Male', meiosisValue=2, parentCluster='Cluster1')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ancestry_matchtrees_columns(session):
    # Check columns in Ancestry_matchTrees table
    instance = Ancestry_matchTrees(matchid='M1', surname='Doe', given='John', birthdate='1970-01-01',
                                   deathdate='2023-01-01', birthplace='New York', deathplace='New York',
                                   relid='R1', personId='P1', fatherId='P2', motherId='P3', source='Ancestry',
                                   created_date='2023-01-01', loginUsername='user1', sync='Yes', birthdt1=1234567890,
                                   birthdt2=1234567899, deathdt1=1234567890, deathdt2=1234567890)
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ancestry_profiles_columns(session):
    # Check columns in Ancestry_Profiles table
    instance = Ancestry_Profiles(guid='G1', name='Test Profile')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ancestry_treedata_columns(session):
    # Check columns in Ancestry_TreeData table
    instance = Ancestry_TreeData(TestGuid='T1', TreeSize=100, PublicTree=1, PrivateTree=0, UnlinkedTree=0,
                                 TreeId='Tree1', NoTrees=1, TreeUnavailable=0)
    session.add(instance)
    session.commit()
    assert instance.Id is not None


if __name__ == '__main__':
    pytest.main()
