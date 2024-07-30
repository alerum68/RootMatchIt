from sqlalchemy import Column, Integer, Float, Index, String, UniqueConstraint
from sqlalchemy.orm import declarative_base

MH_Base = declarative_base()


class MH_Ancestors(MH_Base):
    __tablename__ = 'MH_Ancestors'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    TreeId = Column(Integer)
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
    gender = Column(String)
    dnaLink = Column(String)
    __table_args__ = (
        Index('IDX_MH_Ancestors', 'TreeId'),
        Index('IDX_MH_Ancestors2', 'matchid'),
    )


class MH_Chromo(MH_Base):
    __tablename__ = 'MH_Chromo'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String)
    guid1 = Column(String)
    guid2 = Column(String)
    chromosome = Column(Integer)
    cm = Column(Float)
    start = Column(Integer)
    end = Column(Integer)
    snps = Column(Integer)
    startrs = Column(String)
    endrs = Column(String)
    GF_Sync = Column(String)

    __table_args__ = (
        UniqueConstraint('guid', 'chromosome', 'start', name='IDX_MH_Chromo'),
    )


class MH_ICW(MH_Base):
    __tablename__ = 'MH_ICW'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    id1 = Column(String)
    id2 = Column(String)
    totalCM = Column(Float)
    percent_shared = Column(Float)
    num_segments = Column(Integer)
    triTotalCM = Column(Float)
    triSegments = Column(Integer)
    GF_Sync = Column(String)

    __table_args__ = (
        UniqueConstraint('id2', 'id1', name='IDX_MH_ICW2'),
        UniqueConstraint('id1', 'id2', name='IDX_MH_ICW1'),
    )


class MH_Match(MH_Base):
    __tablename__ = 'MH_Match'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    kitId = Column(Integer)
    guid = Column(String, unique=True)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    age = Column(String)
    birth_place = Column(String)
    gender = Column(String)
    country = Column(String)
    creatorid = Column(String)
    contact = Column(String)
    manager = Column(String)
    contact_manager = Column(String)
    status = Column(String)
    estimated_relationship = Column(String)
    totalCM = Column(Float)
    percent_shared = Column(Float)
    num_segments = Column(Integer)
    largestCM = Column(Float)
    has_tree = Column(Integer)
    tree_size = Column(Integer)
    tree_url = Column(String)
    person_url = Column(String)
    smart_matches = Column(Integer)
    shared_surnames = Column(String)
    surnames = Column(String)
    notes = Column(String)
    CreatedDate = Column(String)
    icwRunDate = Column(String)
    icwcMlow = Column(Float)
    triagRunDate = Column(String)
    treeRunDate = Column(String)
    chromoRunDate = Column(String)
    gmUploadDate = Column(String)
    testid = Column(String)
    matchid = Column(String)
    GF_Sync = Column(String)

    __table_args__ = (
        Index('IDX_MH_MATCH', 'guid'),
    )


class MH_Tree(MH_Base):
    __tablename__ = 'MH_Tree'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    treeurl = Column(String, unique=True)
    created_date = Column(String)
    updated_date = Column(String)

    __table_args__ = (
        UniqueConstraint('treeurl', name='IDX_MH_Tree'),
    )
