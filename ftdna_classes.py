from sqlalchemy import create_engine, Column, Integer, Float, Text, Index, BigInteger, event
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

class DGIndividual(Base):
	    __tablename__ = 'DGIndividual'
	    Id = Column(String, primary_key=True)
	    treeid = Column(String)
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
	    birthdt1 = Column(String)
	    birthdt2 = Column(String)
	    deathdt1 = Column(String)
	    deathdt2 = Column(String)

class DGTree(Base):
    __tablename__ = 'DGTree'
    Id = Column(String, primary_key=True)
    name = Column(String)
    treeid = Column(String)
    treeurl = Column(String)
    basePersonId = Column(String)
    CreateDate = Column(String)
    UpdateDate = Column(String)
    matchID = Column(String)
    source = Column(String)

class DNA_Kits(Base):
    __tablename__ = 'DNA_Kits'
    Id = Column(String, primary_key=True)
    company = Column(String)
    guid = Column(String)
    name = Column(String)
    id2 = Column(String)
    id3 = Column(String)
    created_date = Column(String)

class FTDNA_Matches2(Base):
    __tablename__ = 'FTDNA_Matches2'
    UniqueID = Column(String, primary_key=True)
    Id = Column(String)
    DNAProvider = Column(String)
    Surname = Column(String)
    Given = Column(String)
    ID1 = Column(String)
    ID2 = Column(String)
    Group = Column(String)
    SharedCM = Column(Float)
    SharedSegments = Column(Integer)
    LastLoggedInDate = Column(String)
    Starred = Column(Boolean)
    Viewed = Column(Boolean)
    MatchTreeIsPrivate = Column(Boolean)
    Note = Column(String)
    UserPhoto = Column(String)
    MatchTreeId = Column(String)
    TreeId = Column(String)
    CreatedDate = Column(String)
    LoginUsername = Column(String)
    Sync = Column(Boolean)
    Paternal = Column(Boolean)
    Maternal = Column(Boolean)
    Sex = Column(String)
    MeiosisValue = Column(Float)
    ParentCluster = Column(String)
