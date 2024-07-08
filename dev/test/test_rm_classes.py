import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dev.classes.rm_classes import Base, ChildTable, DNATable, EventTable, FactTypeTable, FamilyTable, GroupTable, NameTable, \
    PersonTable, PlaceTable, URLTable


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
    assert 'ChildTable' in Base.metadata.tables
    assert 'DNATable' in Base.metadata.tables
    assert 'EventTable' in Base.metadata.tables
    assert 'FactTypeTable' in Base.metadata.tables
    assert 'FamilyTable' in Base.metadata.tables
    assert 'GroupTable' in Base.metadata.tables
    assert 'NameTable' in Base.metadata.tables
    assert 'PersonTable' in Base.metadata.tables
    assert 'PlaceTable' in Base.metadata.tables
    assert 'URLTable' in Base.metadata.tables


def test_childtable_columns(session):
    # Check columns in ChildTable
    instance = ChildTable(ChildID=1, FamilyID=1, ChildOrder=1, IsPrivate=0, ProofFather=0, ProofMother=0, Note='Test '
                                                                                                               'Note')
    session.add(instance)
    session.commit()
    assert instance.RecID is not None


def test_dnatable_columns(session):
    # Check columns in DNATable
    instance = DNATable(ID1=1, ID2=2, Label1='Label1', Label2='Label2', DNAProvider=1, SharedCM=100.0,
                        SharedPercent=10.0, LargeSeg=50.0, SharedSegs=1, Date='2023-01-01', Relate1=1, Relate2=2,
                        CommonAnc=0, CommonAncType=0, Verified=1, Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.RecID is not None


def test_eventtable_columns(session):
    # Check columns in EventTable
    instance = EventTable(EventType=1, OwnerType=1, OwnerID=1, FamilyID=1, PlaceID=1, SiteID=1, Date='2023-01-01',
                          SortDate=1234567890, IsPrimary=1, IsPrivate=0, Proof=0, Status=1, Sentence='Test Sentence',
                          Details='Test Details', Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.EventID is not None


def test_facttypetable_columns(session):
    # Check columns in FactTypeTable
    instance = FactTypeTable(OwnerType=1, Name='Test Name', Abbrev='TN', GedcomTag='TAG', UseValue=1, UseDate=1,
                             UsePlace=1, Sentence='Test Sentence', Flags=1)
    session.add(instance)
    session.commit()
    assert instance.FactTypeID is not None


def test_familytable_columns(session):
    # Check columns in FamilyTable
    instance = FamilyTable(FatherID=1, MotherID=2, ChildID=3, HusbOrder=1, WifeOrder=1, IsPrivate=0, Proof=0,
                           SpouseLabel=1, FatherLabel=1, MotherLabel=1, SpouseLabelStr='Husband',
                           FatherLabelStr='Father', MotherLabelStr='Mother', Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.FamilyID is not None


def test_grouptable_columns(session):
    # Check columns in GroupTable
    instance = GroupTable(GroupID=1, StartID=1, EndID=2)
    session.add(instance)
    session.commit()
    assert instance.RecID is not None


def test_nametable_columns(session):
    # Check columns in NameTable
    instance = NameTable(OwnerID=1, Surname='Doe', Given='John', Prefix='Mr.', Suffix='Jr.', Nickname='Johnny',
                         NameType=1, Date='2023-01-01', SortDate=1234567890, IsPrimary=1, IsPrivate=0, Proof=0,
                         Sentence='Test Sentence', Note='Test Note', BirthYear=1970, DeathYear=2023, Display=1,
                         Language='English')
    session.add(instance)
    session.commit()
    assert instance.NameID is not None


def test_persontable_columns(session):
    # Check columns in PersonTable
    instance = PersonTable(UniqueID='U123', Sex=1, ParentID=1, SpouseID=2, Living=1, IsPrivate=0, Proof=0,
                           Bookmark=1, Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.PersonID is not None


def test_placetable_columns(session):
    # Check columns in PlaceTable
    instance = PlaceTable(PlaceType=1, Name='New York', Abbrev='NY', Normalized='new_york', Latitude=40,
                          Longitude=-74, LatLongExact=1, Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.PlaceID is not None


def test_urltable_columns(session):
    # Check columns in URLTable
    instance = URLTable(OwnerType=1, OwnerID=1, LinkType=1, Name='Test URL', URL='http://example.com', Note='Test Note')
    session.add(instance)
    session.commit()
    assert instance.LinkID is not None


if __name__ == '__main__':
    pytest.main()
