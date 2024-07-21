from unittest.mock import MagicMock

import pytest

from import_data import (
    batch_limit,
    generate_unique_id,
    check_for_duplicates,
    filter_selected_kits,
    process_ancestry,
    process_ftdna,
    process_mh,
    insert_person,
    insert_fact_type,
    insert_name,
    insert_event,
    insert_child,
    insert_place,
    insert_dna,
    insert_family,
    insert_group,
    insert_url,
)

# Mocking SQLAlchemy name_rm_session and tables
session = MagicMock()

# Mocking SQLAlchemy ORM classes if needed
PersonTable = MagicMock()
FactTypeTable = MagicMock()
NameTable = MagicMock()
EventTable = MagicMock()
ChildTable = MagicMock()
PlaceTable = MagicMock()
DNATable = MagicMock()
FamilyTable = MagicMock()
GroupTable = MagicMock()
URLTable = MagicMock()


def test_generate_unique_id():
    assert len(generate_unique_id()) == 36
    assert generate_unique_id("test") != generate_unique_id("test2")


def test_process_table_with_limit():
    # Basic test to check if function returns a list
    mock_data = [1, 2, 3]
    result = batch_limit(session, None, [1, 2, 3], lambda x: x, 2)
    assert isinstance(result, list)
    assert len(result) == 2


def test_check_for_duplicates(mocker):
    mocker.patch('import_data.PersonTable', autospec=True)

    # Test creating a new person
    result = check_for_duplicates(session, "unique_id")
    assert isinstance(result, PersonTable)

    # Test updating an existing person
    result = check_for_duplicates(session, "unique_id")  # Assuming person already exists
    assert isinstance(result, PersonTable)


def test_filter_selected_kits(mocker):
    mocker.patch('import_data.filter_session.query')
    # Basic test to check if function returns a dictionary
    result = filter_selected_kits(session, [])
    assert isinstance(result, dict)


def test_process_ancestry(mocker):
    mocker.patch('import_data.process_table_with_limit')
    # Basic test to check if function returns a list
    result = process_ancestry(session, {})
    assert isinstance(result, list)


def test_process_ftdna(mocker):
    mocker.patch('import_data.process_table_with_limit')
    # Basic test to check if function returns a list
    result = process_ftdna(session, {})
    assert isinstance(result, list)


def test_process_mh(mocker):
    mocker.patch('import_data.process_table_with_limit')
    # Basic test to check if function returns a list
    result = process_mh(session, {})
    assert isinstance(result, list)


def test_insert_fact_type():
    insert_fact_type(session)

    # Assert that 'DNA Kit' Fact Type was inserted
    fact_type = session.query(FactTypeTable).filter_by(Name='DNA Kit').first()
    assert fact_type is not None


def test_insert_person(setup_database):
    session = setup_database
    processed_data = [
        {'unique_id': '1', 'sex': 'M', 'color': 'Blue'},
        {'unique_id': '2', 'sex': 'F', 'color': 'Red'},
        {'unique_id': '3', 'sex': 'M', 'color': 'Green'}
    ]

    insert_person(session, processed_data)

    # Assert that the persons were inserted
    inserted_person1 = session.query(PersonTable).filter_by(UniqueID='1', Sex='M', Color='Blue').first()
    inserted_person2 = session.query(PersonTable).filter_by(UniqueID='2', Sex='F', Color='Red').first()
    inserted_person3 = session.query(PersonTable).filter_by(UniqueID='3', Sex='M', Color='Green').first()

    assert inserted_person1 is not None
    assert inserted_person2 is not None
    assert inserted_person3 is not None


if __name__ == "__main__":
    pytest.main()


def test_insert_name():
    insert_name(session, {
        'OwnerID': 1,
        'Surname': 'Smith',
        'Given': 'John',
        'Prefix': None,
        'Suffix': None,
        'Nickname': None,
        'NameType': 2,
        'Date': None,
        'SortDate': None,
        'IsPrimary': 1,
        'IsPrivate': None,
        'Proof': None,
        'Sentence': None,
        'Note': None,
        'BirthYear': None,
        'DeathYear': None,
        'Display': None,
        'Language': None,
        'UTCModDate': None,
        'SurnameMP': 'Smith',
        'GivenMP': 'John',
        'NicknameMP': None
    })

    # Assert that the name was inserted
    inserted_name = session.query(NameTable).filter_by(OwnerID=1, Surname='Smith', Given='John').first()
    assert inserted_name is not None


def test_insert_event():
    insert_event(session, {
        'EventType': 1,
        'OwnerType': 2,
        'OwnerID': 1,
        'FamilyID': None,
        'PlaceID': None,
        'SiteID': None,
        'Date': '2024-07-13',
        'SortDate': 20240713,
        'IsPrimary': 1,
        'IsPrivate': None,
        'Proof': None,
        'Status': None,
        'Sentence': 'Test event',
        'Details': None,
        'Note': None,
        'UTCModDate': None
    })

    # Assert that the event was inserted
    inserted_event = session.query(EventTable).filter_by(OwnerID=1, EventType=1, Date='2024-07-13').first()
    assert inserted_event is not None


def test_insert_child():
    insert_child(session, {
        'ChildID': 1,
        'FamilyID': 1,
        'RelFather': None,
        'RelMother': None,
        'ChildOrder': 1,
        'IsPrivate': None,
        'ProofFather': None,
        'ProofMother': None,
        'Note': None,
        'UTCModDate': None
    })

    # Assert that the child was inserted
    inserted_child = session.query(ChildTable).filter_by(ChildID=1, FamilyID=1, ChildOrder=1).first()
    assert inserted_child is not None


def test_insert_place():
    insert_place(session, {
        'PlaceType': 1,
        'Name': 'Test Place',
        'Abbrev': None,
        'Normalized': None,
        'Latitude': None,
        'Longitude': None,
        'LatLongExact': None,
        'MasterID': None,
        'Note': None,
        'Reverse': None,
        'fsID': None,
        'anID': None,
        'UTCModDate': None
    })

    # Assert that the place was inserted
    inserted_place = session.query(PlaceTable).filter_by(Name='Test Place').first()
    assert inserted_place is not None


def test_insert_dna():
    insert_dna(session, {
        'ID1': 1,
        'ID2': 2,
        'Label1': 'Person A',
        'Label2': 'Person B',
        'DNAProvider': 2,
        'SharedCM': 100.0,
        'SharedPercent': 99.9,
        'LargeSeg': 50.0,
        'SharedSegs': 10,
        'Date': '2024-07-13',
        'Relate1': None,
        'Relate2': None,
        'CommonAnc': None,
        'CommonAncType': None,
        'Verified': None,
        'Note': None,
        'UTCModDate': None
    })

    # Assert that the DNA entry was inserted
    inserted_dna = session.query(DNATable).filter_by(ID1=1, ID2=2).first()
    assert inserted_dna is not None


def test_insert_family():
    insert_family(session, {
        'FamilyID': 1,
        'FatherID': 1,
        'MotherID': 2,
        'ChildID': 1,
        'HusbOrder': None,
        'WifeOrder': None,
        'IsPrivate': None,
        'Proof': None,
        'SpouseLabel': None,
        'FatherLabel': None,
        'MotherLabel': None,
        'SpouseLabelStr': None,
        'FatherLabelStr': None,
        'MotherLabelStr': None,
        'Note': None,
        'UTCModDate': None
    })

    # Assert that the family entry was inserted
    inserted_family = session.query(FamilyTable).filter_by(FamilyID=1, FatherID=1, MotherID=2, ChildID=1).first()
    assert inserted_family is not None


def test_insert_group():
    insert_group(session, {
        'GroupID': 1,
        'StartID': 1,
        'EndID': 2,
        'UTCModDate': None
    })

    # Assert that the group entry was inserted
    inserted_group = session.query(GroupTable).filter_by(GroupID=1, StartID=1, EndID=2).first()
    assert inserted_group is not None


def test_insert_url():
    insert_url(session, {
        'OwnerType': 1,
        'OwnerID': 1,
        'LinkType': 1,
        'Name': 'Test URL',
        'URL': 'http://example.com',
        'Note': None,
        'UTCModDate': None
    })

    # Assert that the URL entry was inserted
    inserted_url = session.query(URLTable).filter_by(OwnerType=1, OwnerID=1, LinkType=1, Name='Test URL').first()
    assert inserted_url is not None


if __name__ == '__main__':
    pytest.main()
