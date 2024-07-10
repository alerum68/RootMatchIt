from sqlalchemy import Column, Integer, Float, Index, BigInteger, String
from sqlalchemy.orm import declarative_base

FTDNA_Base = declarative_base()


class DGTree(FTDNA_Base):
    __tablename__ = 'DGTree'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    treeid = Column(String)
    treeurl = Column(String)
    basePersonId = Column(String)
    CreateDate = Column(BigInteger)
    UpdateDate = Column(BigInteger)
    matchID = Column(String)
    source = Column(String)
    __table_args__ = (
        Index('IDX_DGTreeSource', 'source'),
        Index('IDX_DGTreeID', 'Id'),
    )


class FTDNA_Chromo2(FTDNA_Base):
    __tablename__ = 'FTDNA_Chromo2'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    eKit1 = Column(String)
    eKit2 = Column(String)
    chromosome = Column(Integer)
    cmfloat = Column(Float)
    p1 = Column(Integer)
    p2 = Column(Integer)
    snpsI = Column(Integer)
    created_date = Column(String)
    GF_Sync = Column(String)
    __table_args__ = (
        Index('IDX_FTDNA_Chromo12', 'eKit1'),
        Index('IDX_FTDNA_ChromoB2', 'eKit1', 'eKit2'),
        Index('IDX_FTDNA_Chromo22', 'eKit2'),
    )


class FTDNA_ICW2(FTDNA_Base):
    __tablename__ = 'FTDNA_ICW2'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    eKitKit = Column(String)
    eKitMatch1 = Column(String)
    eKitMatch2 = Column(String)
    created_date = Column(String)
    GF_Sync = Column(String)
    __table_args__ = (
        Index('IDX_FTDNA_ICW_RRI2', 'eKitKit', 'eKitMatch1', 'eKitMatch2'),
        Index('IDX_FTDNA_ICW12', 'eKitKit'),
        Index('IDX_FTDNA_ICW_RI2', 'eKitKit', 'eKitMatch2'),
        Index('IDX_FTDNA_ICW32', 'eKitMatch2'),
        Index('IDX_FTDNA_ICW22', 'eKitMatch1'),
        Index('IDX_FTDNA_ICW_RR2', 'eKitKit', 'eKitMatch1'),
    )


class FTDNA_Matches2(FTDNA_Base):
    __tablename__ = 'FTDNA_Matches2'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    eKit1 = Column(String)
    eKit2 = Column(String)
    Name = Column(String)
    MatchPersonName = Column(String)
    Prefix = Column(String)
    FirstName = Column(String)
    MiddleName = Column(String)
    LastName = Column(String)
    Suffix = Column(String)
    Email = Column(String)
    HasFamilyTree = Column(String)
    ThirdParty = Column(String)
    Note = Column(String)
    Female = Column(String)
    contactId = Column(String)
    AboutMe = Column(String)
    PaternalAncestorName = Column(String)
    MaternalAncestorName = Column(String)
    strRbdate = Column(String)
    Relationship = Column(String)
    strRelationshipRange = Column(String)
    strSuggestedRelationship = Column(String)
    strRelationshipName = Column(String)
    totalCM = Column(String)
    userSurnames = Column(String)
    longestCentimorgans = Column(String)
    ydnaMarkers = Column(String)
    mtDNAMarkers = Column(String)
    yHaplo = Column(String)
    mtHaplo = Column(String)
    isxMatch = Column(String)
    ffBack = Column(String)
    bucketType = Column(String)
    nRownum = Column(String)
    familyTreeUrl = Column(String)
    created_date = Column(String)
    icw_date = Column(BigInteger)
    icw_tree = Column(BigInteger)
    totalCMf = Column(Float)
    longestCentimorgansf = Column(Float)
    GF_Sync = Column(String)


class DGIndividual(FTDNA_Base):
    __tablename__ = 'DGIndividual'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    treeid = Column(Integer)
    matchid = Column(String)
    surname = Column(String)
    given = Column(String)
    birthdate = Column(String)
    deathdate = Column(String)
    birthplace = Column(String)
    deathplace = Column(String)
    sex = Column(String)
    relid = Column(String)
    personId = Column(String)
    fatherId = Column(String)
    motherId = Column(String)
    spouseId = Column(String)
    source = Column(String)
    created_date = Column(String)
    birthdt1 = Column(BigInteger)
    birthdt2 = Column(BigInteger)
    deathdt1 = Column(BigInteger)
    deathdt2 = Column(BigInteger)
    __table_args__ = (
        Index('IDX_DGIndividualTM', 'treeid', 'matchid'),
        Index('IDX_DGIndividualSM', 'source', 'matchid'),
    )
