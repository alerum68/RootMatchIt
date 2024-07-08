from sqlalchemy import Column, Integer, Float, Text, Index, BigInteger
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ChildTable(Base):
    # Define RootsMagic ChildTable
    __tablename__ = 'ChildTable'

    RecID = Column(Integer, primary_key=True)
    ChildID = Column(Integer)
    FamilyID = Column(Integer)
    RelFather = Column(Integer)
    RelMother = Column(Integer)
    ChildOrder = Column(Integer)
    IsPrivate = Column(Integer)
    ProofFather = Column(Integer)
    ProofMother = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for ChildTable

    Index('idxChildOrder', ChildOrder)
    Index('idxChildID', ChildID)
    Index('idxChildFamilyID', FamilyID)


class DNATable(Base):
    # Define RootsMagic DNATable
    __tablename__ = 'DNATable'

    RecID = Column(Integer, primary_key=True)
    ID1 = Column(Integer)
    ID2 = Column(Integer)
    Label1 = Column(Text)
    Label2 = Column(Text)
    DNAProvider = Column(Integer)
    SharedCM = Column(Float)
    SharedPercent = Column(Float)
    LargeSeg = Column(Float)
    SharedSegs = Column(Integer)
    Date = Column(Text)
    Relate1 = Column(Integer)
    Relate2 = Column(Integer)
    CommonAnc = Column(Integer)
    CommonAncType = Column(Integer)
    Verified = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for DNATable
    idxDnaId2 = Index('idxDnaId2', ID2)
    idxDnaId1 = Index('idxDnaId1', ID1)


class EventTable(Base):
    # Define RootsMagic EventTable
    __tablename__ = 'EventTable'
    EventID = Column(Integer, primary_key=True)
    EventType = Column(Integer)
    OwnerType = Column(Integer)
    OwnerID = Column(Integer)
    FamilyID = Column(Integer)
    PlaceID = Column(Integer)
    SiteID = Column(Integer)
    Date = Column(Text)
    SortDate = Column(BigInteger)
    IsPrimary = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Status = Column(Integer)
    Sentence = Column(Text)
    Details = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for EventTable
    idxOwnerEvent = Index('idxOwnerEvent', OwnerID, EventType)
    idxOwnerDate = Index('idxOwnerDate', OwnerID, SortDate)


class FactTypeTable(Base):
    # Define RootsMagic FactTypeTable
    __tablename__ = 'FactTypeTable'
    FactTypeID = Column(Integer, primary_key=True)
    OwnerType = Column(Integer)
    Name = Column(Text)
    Abbrev = Column(Text)
    GedcomTag = Column(Text)
    UseValue = Column(Integer)
    UseDate = Column(Integer)
    UsePlace = Column(Integer)
    Sentence = Column(Text)
    Flags = Column(Integer)
    UTCModDate = Column(Float)

    # Define indices for FactTypeTable
    idxFactTypeName = Index('idxFactTypeName', Name)
    idxFactTypeAbbrev = Index('idxFactTypeAbbrev', Abbrev)
    idxFactTypeGedcomTag = Index('idxFactTypeGedcomTag', GedcomTag)


class FamilyTable(Base):
    # Define RootsMagic FamilyTable
    __tablename__ = 'FamilyTable'
    FamilyID = Column(Integer, primary_key=True)
    FatherID = Column(Integer)
    MotherID = Column(Integer)
    ChildID = Column(Integer)
    HusbOrder = Column(Integer)
    WifeOrder = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    SpouseLabel = Column(Integer)
    FatherLabel = Column(Integer)
    MotherLabel = Column(Integer)
    SpouseLabelStr = Column(Text)
    FatherLabelStr = Column(Text)
    MotherLabelStr = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for FamilyTable
    idxFamilyMotherID = Index('idxFamilyMotherID', MotherID)
    idxFamilyFatherID = Index('idxFamilyFatherID', FatherID)


class GroupTable(Base):
    # Define RootsMagic GroupTable
    __tablename__ = 'GroupTable'
    RecID = Column(Integer, primary_key=True)
    GroupID = Column(Integer)
    StartID = Column(Integer)
    EndID = Column(Integer)
    UTCModDate = Column(Float)


class NameTable(Base):
    # Define RootsMagic NameTable
    __tablename__ = 'NameTable'
    NameID = Column(Integer, primary_key=True)
    OwnerID = Column(Integer)
    Surname = Column(Text)
    Given = Column(Text)
    Prefix = Column(Text)
    Suffix = Column(Text)
    Nickname = Column(Text)
    NameType = Column(Integer)
    Date = Column(Text)
    SortDate = Column(Integer)
    IsPrimary = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Sentence = Column(Text)
    Note = Column(Text)
    BirthYear = Column(Integer)
    DeathYear = Column(Integer)
    Display = Column(Integer)
    Language = Column(Text)
    UTCModDate = Column(Float)
    SurnameMP = Column(Text)
    GivenMP = Column(Text)
    NicknameMP = Column(Text)

    # Define indices for NameTable
    idxSurnameGiven = Index('idxSurnameGiven', Surname, Given, BirthYear, DeathYear)
    idxSurnameGivenMP = Index('idxSurnameGivenMP', SurnameMP, GivenMP, BirthYear, DeathYear)
    idxNamePrimary = Index('idxNamePrimary', IsPrimary)
    idxGivenMP = Index('idxGivenMP', GivenMP)
    idxNameOwnerID = Index('idxNameOwnerID', OwnerID)
    idxGiven = Index('idxGiven', Given)
    idxSurname = Index('idxSurname', Surname)
    idxSurnameMP = Index('idxSurnameMP', SurnameMP)


class PersonTable(Base):
    # Define RootsMagic PersonTable
    __tablename__ = 'PersonTable'
    PersonID = Column(Integer, primary_key=True)
    UniqueID = Column(Text)
    Sex = Column(Integer)
    ParentID = Column(Integer)
    SpouseID = Column(Integer)
    Color = Column(Integer)
    Color1 = Column(Integer)
    Color2 = Column(Integer)
    Color3 = Column(Integer)
    Color4 = Column(Integer)
    Color5 = Column(Integer)
    Color6 = Column(Integer)
    Color7 = Column(Integer)
    Color8 = Column(Integer)
    Color9 = Column(Integer)
    Relate1 = Column(Integer)
    Relate2 = Column(Integer)
    Flags = Column(Integer)
    Living = Column(Integer)
    IsPrivate = Column(Integer)
    Proof = Column(Integer)
    Bookmark = Column(Integer)
    Note = Column(Text)
    UTCModDate = Column(Float)


class PlaceTable(Base):
    # Define RootsMagic PlaceTable
    __tablename__ = 'PlaceTable'
    PlaceID = Column(Integer, primary_key=True)
    PlaceType = Column(Integer)
    Name = Column(Text)
    Abbrev = Column(Text)
    Normalized = Column(Text)
    Latitude = Column(Integer)
    Longitude = Column(Integer)
    LatLongExact = Column(Integer)
    MasterID = Column(Integer)
    Note = Column(Text)
    Reverse = Column(Text)
    fsID = Column(Integer)
    anID = Column(Integer)
    UTCModDate = Column(Float)

    # Define indices for PlaceTable
    idxPlaceName = Index('idxPlaceName', Name)
    idxPlaceAbbrev = Index('idxPlaceAbbrev', Abbrev)
    idxReversePlaceName = Index('idxReversePlaceName', Reverse)


class URLTable(Base):
    # Define RootsMagic URLTable
    __tablename__ = 'URLTable'
    LinkID = Column(Integer, primary_key=True)
    OwnerType = Column(Integer)
    OwnerID = Column(Integer)
    LinkType = Column(Integer)
    Name = Column(Text)
    URL = Column(Text)
    Note = Column(Text)
    UTCModDate = Column(Float)

    # Define indices for URLTable
    idxUrlOwnerID = Index('idxUrlOwnerID', OwnerID)
    idxUrlOwnerType = Index('idxUrlOwnerType', OwnerType)
