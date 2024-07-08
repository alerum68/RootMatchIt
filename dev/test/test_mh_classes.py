import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dev.classes.mh_classes import Base, MH_Ancestors, MH_Chromo, MH_ICW, MH_Match, MH_Tree


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
    # Check if classes are defined and part of Base metadata
    assert 'MH_Ancestors' in Base.metadata.tables
    assert 'MH_Chromo' in Base.metadata.tables
    assert 'MH_ICW' in Base.metadata.tables
    assert 'MH_Match' in Base.metadata.tables
    assert 'MH_Tree' in Base.metadata.tables


def test_mh_ancestors_columns(session):
    # Check columns in MH_Ancestors table
    instance = MH_Ancestors(TreeId=1, matchid='M1', surname='Doe', given='John', birthdate='1970-01-01',
                            deathdate='2023-01-01', birthplace='New York', deathplace='New York',
                            relid='R123', personId='P123', fatherId='P456', motherId='P789', gender='Male',
                            dnaLink='http://example.com')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_mh_chromo_columns(session):
    # Check columns in MH_Chromo table
    instance = MH_Chromo(guid='G123', guid1='G1', guid2='G2', chromosome=1, cm=10.5, start=100, end=200, snps=500,
                         startrs='123-456', endrs='456-789', GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_mh_icw_columns(session):
    # Check columns in MH_ICW table
    instance = MH_ICW(id1='ID1', id2='ID2', totalCM=100.5, percent_shared=50.0, num_segments=5, triTotalCM=200.0,
                      triSegments=10, GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_mh_match_columns(session):
    # Check columns in MH_Match table
    instance = MH_Match(guid='M123', name='Test Match', first_name='John', last_name='Doe', gender='Male',
                        totalCM=100.5, percent_shared=50.0, num_segments=5, largestCM=50.0, has_tree=1,
                        tree_url='http://example.com', person_url='http://example.com/person',
                        smart_matches=1, shared_surnames='Doe', surnames='Doe', GF_Sync='Yes')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


def test_mh_tree_columns(session):
    # Check columns in MH_Tree table
    instance = MH_Tree(treeurl='http://example.com/tree', created_date='2023-01-01', updated_date='2023-01-02')
    session.add(instance)
    session.commit()
    assert instance.Id is not None


if __name__ == '__main__':
    pytest.main()
