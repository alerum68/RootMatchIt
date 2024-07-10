import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from dev.classes.ftdna_classes import Base, DGTree, FTDNA_Chromo2, FTDNA_ICW2, FTDNA_Matches2, DGIndividual


# Set up the SQLite in-memory database for testing
@pytest.fixture(scope='module')
def engine():
    return create_engine('sqlite:///:memory:')


@pytest.fixture(scope='module')
def tables(engine):
    # Create tables in the in-memory database
    Base.metadata.create_all(engine)
    yield
    # Drop tables after the test
    Base.metadata.drop_all(engine)


@pytest.fixture(scope='module')
def session(engine, tables):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_class_definitions():
    # Check if classes are defined and part of Ancestry_Base metadata
    assert 'DGTree' in Base.metadata.tables
    assert 'FTDNA_Chromo2' in Base.metadata.tables
    assert 'FTDNA_ICW2' in Base.metadata.tables
    assert 'FTDNA_Matches2' in Base.metadata.tables
    assert 'DGIndividual' in Base.metadata.tables


def test_table_creation(engine, tables):
    # Check if tables can be created
    inspector = inspect(engine)
    assert inspector.has_table('DGTree')
    assert inspector.has_table('FTDNA_Chromo2')
    assert inspector.has_table('FTDNA_ICW2')
    assert inspector.has_table('FTDNA_Matches2')
    assert inspector.has_table('DGIndividual')


def test_dgtree_columns(session):
    # Check columns in DGTree table
    instance = DGTree(name='Test Tree', treeid='T123', treeurl='http://example.com', basePersonId='P456',
                      CreateDate=1234567890, UpdateDate=1234567899, matchID='M789', source='FTDNA')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ftdna_chromo2_columns(session):
    # Check columns in FTDNA_Chromo2 table
    instance = FTDNA_Chromo2(eKit1='E1', eKit2='E2', chromosome=1, cmfloat=10.5, p1=100, p2=200, snpsI=500,
                             created_date='2023-01-01', GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ftdna_icw2_columns(session):
    # Check columns in FTDNA_ICW2 table
    instance = FTDNA_ICW2(eKitKit='E1', eKitMatch1='M1', eKitMatch2='M2', created_date='2023-01-01', GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_ftdna_matches2_columns(session):
    # Check columns in FTDNA_Matches2 table
    instance = FTDNA_Matches2(eKit1='E1', eKit2='E2', Name='Test Match', MatchPersonName='Test Person',
                              Prefix='Mr.', FirstName='John', MiddleName='A.', LastName='Doe', Suffix='Jr.',
                              Email='test@example.com', HasFamilyTree='Yes', ThirdParty='Ancestry',
                              Note='Test note', Female='No', contactId='C123', AboutMe='Test about me',
                              PaternalAncestorName='John Doe Sr.', MaternalAncestorName='Jane Doe',
                              strRbdate='1970-01-01', Relationship='2nd Cousin',
                              strRelationshipRange='2nd to 3rd Cousin',
                              strSuggestedRelationship='2nd Cousin', strRelationshipName='Cousin',
                              totalCM='100', userSurnames='Doe', longestCentimorgans='50',
                              ydnaMarkers='Y-DNA12', mtDNAMarkers='HVR1', yHaplo='R-M269', mtHaplo='H1',
                              isxMatch='Yes', ffBack='Yes', bucketType='Family', nRownum='1',
                              familyTreeUrl='http://example.com',
                              created_date='2023-01-01', icw_date=1234567890, icw_tree=9876543210,
                              totalCMf=100.5, longestCentimorgansf=50.5, GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_dgindividual_columns(session):
    # Check columns in DGIndividual table
    instance = DGIndividual(treeid=1, matchid='M1', surname='Doe', given='John', birthdate='1970-01-01',
                            deathdate='2023-01-01', birthplace='New York', deathplace='New York', sex='Male',
                            relid='R123', personId='P123', fatherId='P456', motherId='P789', spouseId='PABC',
                            source='FTDNA', created_date='2023-01-01', birthdt1=1234567890, birthdt2=1234567899,
                            deathdt1=1234567890, deathdt2=1234567890)
    session.add(instance)
    session.commit()
    assert instance.Id is not None


if __name__ == '__main__':
    pytest.main()
