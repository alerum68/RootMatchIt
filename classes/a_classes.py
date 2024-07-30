from sqlalchemy import Column, Integer, Float, Index, BigInteger, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Ancestry_Base = declarative_base()


class AncestryAncestorCouple(Ancestry_Base):
    __tablename__ = 'AncestryAncestorCouple'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    TestGuid = Column(String(36))
    MatchGuid = Column(String(36), ForeignKey('Ancestry_matchGroups.matchGuid'))

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

    # Define relationship with Ancestry_matchGroups
    group = relationship("Ancestry_matchGroups", back_populates="ancestor_couples")

    __table_args__ = (
        Index('IDX_AncestryAncestorCouple', 'TestGuid', 'MatchGuid', unique=True),
    )


class Ancestry_ICW(Ancestry_Base):
    __tablename__ = 'Ancestry_ICW'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchid = Column(String, ForeignKey('Ancestry_matchGroups.matchGuid'))
    icwid = Column(String)
    created_date = Column(String)
    sharedCentimorgans = Column(Float)
    confidence = Column(Float)
    meiosis = Column(Integer)
    numSharedSegments = Column(Integer)

    # Define relationship with Ancestry_matchGroups
    group = relationship("Ancestry_matchGroups", back_populates="icw_matches")

    __table_args__ = (
        Index('IDX_Ancestry_ICW2', 'icwid', 'matchid', unique=True),
        Index('IDX_Ancestry_ICW', 'matchid', 'icwid', unique=True),
    )


class Ancestry_Ethnicity(Ancestry_Base):
    __tablename__ = 'Ancestry_Ethnicity'

    code = Column(String, primary_key=True, nullable=False)
    value = Column(String)


class Ancestry_matchEthnicity(Ancestry_Base):
    __tablename__ = 'Ancestry_matchEthnicity'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchGuid = Column(String, ForeignKey('Ancestry_matchGroups.matchGuid'))
    ethnicregions = Column(String)
    ethnictraceregions = Column(String)
    created_date = Column(String)
    percent = Column(Integer)
    version = Column(Integer)

    # Define relationship with Ancestry_matchGroups
    group = relationship("Ancestry_matchGroups", back_populates="match_ethnicity")

    __table_args__ = (
        Index('IDX_Ancestry_matchEthnicity', 'matchGuid', unique=True),
    )


class Ancestry_matchGroups(Ancestry_Base):
    __tablename__ = 'Ancestry_matchGroups'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    testGuid = Column(String(36))
    matchGuid = Column(String(36), unique=True)
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
    loginUsername = Column(String)
    sync = Column(String)
    matchTestAdminUcdmId = Column(String)
    GF_Sync = Column(String)
    paternal = Column(Integer)
    maternal = Column(Integer)
    subjectGender = Column(String)
    meiosisValue = Column(String)
    parentCluster = Column(String)
    matchRunDate = Column(String)

    match_trees = relationship("Ancestry_matchTrees",
                               primaryjoin="Ancestry_matchGroups.matchGuid == Ancestry_matchTrees.matchid",
                               back_populates="group")
    tree_data = relationship("Ancestry_TreeData",
                             primaryjoin="Ancestry_matchGroups.matchGuid == foreign(Ancestry_TreeData.TestGuid)",
                             back_populates="group")
    profile = relationship("Ancestry_Profiles",
                           primaryjoin="Ancestry_matchGroups.testGuid == foreign(Ancestry_Profiles.guid)",
                           back_populates="group")
    icw_matches = relationship("Ancestry_ICW", back_populates="group")
    match_ethnicity = relationship("Ancestry_matchEthnicity", back_populates="group")
    ancestor_couples = relationship("AncestryAncestorCouple", back_populates="group")

    __table_args__ = (
        Index('IDX_Ancestry_matchGroups5', 'matchGuid', 'testGuid', 'sharedCentimorgans', 'groupName', unique=True),
        Index('IDX_Ancestry_matchGroups3', 'testGuid', 'matchGuid', 'groupName', unique=True),
        Index('IDX_Ancestry_matchGroups', 'testGuid', 'matchGuid', unique=True),
        Index('IDX_Ancestry_matchGroups4', 'testGuid', 'matchGuid', 'sharedCentimorgans', unique=True),
        Index('IDX_Ancestry_matchGroups2', 'matchGuid'),
    )


class Ancestry_matchTrees(Ancestry_Base):
    __tablename__ = 'Ancestry_matchTrees'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    matchid = Column(String(36), ForeignKey('Ancestry_matchGroups.matchGuid'))
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

    group = relationship("Ancestry_matchGroups", back_populates="match_trees")

    __table_args__ = (
        Index('IDX_Ancestry_matchTrees', 'matchid', unique=True),
    )


class Ancestry_Profiles(Ancestry_Base):
    __tablename__ = 'Ancestry_Profiles'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String)
    name = Column(String)

    group = relationship("Ancestry_matchGroups",
                         primaryjoin="foreign(Ancestry_Profiles.guid) == Ancestry_matchGroups.testGuid",
                         back_populates="profile")

    __table_args__ = (
        Index('IDX_Ancestry_Profiles', 'guid', unique=True),
    )


class Ancestry_TreeData(Ancestry_Base):
    __tablename__ = 'Ancestry_TreeData'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    TestGuid = Column(String(36), ForeignKey('Ancestry_matchGroups.matchGuid'))
    TreeSize = Column(Integer)
    PublicTree = Column(Integer)
    PrivateTree = Column(Integer)
    UnlinkedTree = Column(Integer)
    TreeId = Column(String)
    NoTrees = Column(Integer)
    TreeUnavailable = Column(Integer)

    group = relationship("Ancestry_matchGroups",
                         primaryjoin="foreign(Ancestry_TreeData.TestGuid) == Ancestry_matchGroups.matchGuid",
                         back_populates="tree_data")

    __table_args__ = (
        Index('IDX_Ancestry_TreeData', 'TestGuid', unique=True),
    )
