from sqlalchemy import Column, Integer, Float, Index, BigInteger, String, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base

Ancestry_Base = declarative_base()


class AncestryAncestorCouple(Ancestry_Base):
    __tablename__ = 'AncestryAncestorCouple'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    TestGuid = Column(String(36))
    MatchGuid = Column(String(36))
    FatherAmtGid = Column(String)
    FatherBigTreeGid = Column(String)
    FatherKinshipPathToSampleId = Column(String)
    FatherKinshipPathFromSampleToMatch = Column(String)
    FatherPotential = Column(Integer)
    FatherInMatchTree = Column(Integer)
    FatherInBestContributorTree = Column(Integer)
    FatherDisplayName = Column(String)
    FatherBirthYear = Column(String)
    FatherDeathYear = Column(String)
    FatherIsMale = Column(Integer)
    FatherNotFound = Column(Integer)
    FatherVeiled = Column(Integer)
    FatherRelationshipToSampleId = Column(String)
    FatherRelationshipFromSampleToMatch = Column(String)
    MotherAmtGid = Column(String)
    MotherBigTreeGid = Column(String)
    MotherKinshipPathToSampleId = Column(String)
    MotherKinshipPathFromSampleToMatch = Column(String)
    MotherPotential = Column(Integer)
    MotherInMatchTree = Column(Integer)
    MotherInBestContributorTree = Column(Integer)
    MotherDisplayName = Column(String)
    MotherBirthYear = Column(String)
    MotherDeathYear = Column(String)
    MotherIsFemale = Column(Integer)
    MotherNotFound = Column(Integer)
    MotherVeiled = Column(Integer)
    MotherRelationshipToSampleId = Column(String)
    MotherRelationshipFromSampleToMatch = Column(String)

    __table_args__ = (
        UniqueConstraint('TestGuid', 'MatchGuid', name='IDX_AncestryAncestorCouple'),
    )


class Ancestry_ICW(Ancestry_Base):
    __tablename__ = 'Ancestry_ICW'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchid = Column(String)
    matchname = Column(String)
    matchadmin = Column(String)
    icwid = Column(String)
    icwname = Column(String)
    icwadmin = Column(String)
    source = Column(String)
    created_date = Column(String)
    loginUsername = Column(String)
    sync = Column(String)
    GF_Sync = Column(String)

    __table_args__ = (
        UniqueConstraint('icwid', 'matchid', name='IDX_Ancestry_ICW2'),
        UniqueConstraint('matchid', 'icwid', name='IDX_Ancestry_ICW'),
    )


class Ancestry_Ethnicity(Ancestry_Base):
    __tablename__ = 'Ancestry_Ethnicity'

    code = Column(String, primary_key=True, nullable=False)
    value = Column(String)


class Ancestry_matchEthnicity(Ancestry_Base):
    __tablename__ = 'Ancestry_matchEthnicity'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchGuid = Column(String)
    ethnicregions = Column(String)
    ethnictraceregions = Column(String)
    created_date = Column(String)
    percent = Column(Integer)
    version = Column(Integer)

    __table_args__ = (
        UniqueConstraint('matchGuid', name='IDX_Ancestry_matchEthnicity'),
    )


class Ancestry_matchGroups(Ancestry_Base):
    __tablename__ = 'Ancestry_matchGroups'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    testGuid = Column(String)
    matchGuid = Column(String)
    matchTestDisplayName = Column(String)
    matchTestAdminDisplayName = Column(String)
    matchTreeNodeCount = Column(Integer)
    groupName = Column(String)
    confidence = Column(Float)
    sharedCentimorgans = Column(Float)
    fetchedSegmentInfo = Column(Integer)
    totalSharedCentimorgans = Column(Float)
    longestSegmentCentimorgans = Column(Float)
    sharedSegment = Column(Integer)
    lastLoggedInDate = Column(String)
    starred = Column(String)
    viewed = Column(String)
    matchTreeIsPrivate = Column(String)
    hasHint = Column(String)
    note = Column(String)
    userPhoto = Column(String)
    matchTreeId = Column(String)
    treeId = Column(String)
    matchMemberSinceYear = Column(String)
    created_date = Column(String)
    icwRunDate = Column(String)
    treeRunDate = Column(String)
    matchRunDate = Column(String)
    loginUsername = Column(String)
    sync = Column(String)
    matchTestAdminUcdmId = Column(String)
    GF_Sync = Column(String)
    paternal = Column(Integer)
    maternal = Column(Integer)
    subjectGender = Column(String)
    meiosisValue = Column(Integer)
    parentCluster = Column(String)

    __table_args__ = (
        UniqueConstraint('matchGuid', 'testGuid', 'sharedCentimorgans', 'groupName', name='IDX_Ancestry_matchGroups5'),
        UniqueConstraint('testGuid', 'matchGuid', 'groupName', name='IDX_Ancestry_matchGroups3'),
        UniqueConstraint('testGuid', 'matchGuid', name='IDX_Ancestry_matchGroups'),
        UniqueConstraint('testGuid', 'matchGuid', 'sharedCentimorgans', name='IDX_Ancestry_matchGroups4'),
        Index('IDX_Ancestry_matchGroups2', 'matchGuid'),
    )


class Ancestry_matchTrees(Ancestry_Base):
    __tablename__ = 'Ancestry_matchTrees'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchid = Column(String)
    surname = Column(String)
    given = Column(String)
    birthdate = Column(String)
    deathdate = Column(String)
    birthplace = Column(String)
    deathplace = Column(String)
    relid = Column(String)
    personId = Column(String)
    fatherId = Column(String)
    motherId = Column(String)
    source = Column(String)
    created_date = Column(String)
    loginUsername = Column(String)
    sync = Column(String)
    birthdt1 = Column(BigInteger)
    birthdt2 = Column(BigInteger)
    deathdt1 = Column(BigInteger)
    deathdt2 = Column(BigInteger)

    __table_args__ = (
        Index('IDX_Ancestry_matchTrees', 'matchid', 'relid'),
    )


class Ancestry_Profiles(Ancestry_Base):
    __tablename__ = 'Ancestry_Profiles'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String)
    name = Column(String)


class Ancestry_TreeData(Ancestry_Base):
    __tablename__ = 'Ancestry_TreeData'
    __table_args__ = (
        ForeignKeyConstraint(['matchGuid'], ['Ancestry_matchGroups.matchGuid']),
        Index('IDX_Ancestry_TreeData', 'TestGuid', 'matchGuid', unique=True),
    )

    Id = Column(Integer, primary_key=True, autoincrement=True)
    TestGuid = Column(String(36))
    TreeSize = Column(Integer)
    PublicTree = Column(Integer)
    PrivateTree = Column(Integer)
    UnlinkedTree = Column(Integer)
    TreeId = Column(String)
    NoTrees = Column(Integer)
    TreeUnavailable = Column(Integer)
    matchGuid = Column(String)  # Define matchGuid as a column here
