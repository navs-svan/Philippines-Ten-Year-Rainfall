from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, DATE, NUMERIC
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import URL, create_engine
import os

Base = declarative_base()

class Places(Base):
    __tablename__ = "places"

    place_id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(128))
    latitude = Column(NUMERIC)
    longitude = Column(NUMERIC)

    rainfall = relationship("Rainfall")

class Dates(Base):
    __tablename__ = "dates"

    date_id = Column(INTEGER, primary_key=True)
    date = Column(DATE)
    year = Column(INTEGER)
    month = Column(INTEGER)

    rainfall = relationship("Rainfall")

class Rainfall(Base):
    __tablename__ = "rainfall"

    place_id = Column(INTEGER, ForeignKey(Places.place_id), primary_key=True)
    date_id = Column(INTEGER, ForeignKey(Dates.date_id), primary_key=True)
    amount = Column(NUMERIC)
    

if __name__ == "__main__":
    # CREATE THE TABLES
    
    DBHOST = os.environ.get("DBHOST")
    DBPORT = os.environ.get("DBPORT")
    DBUSER = os.environ.get("DBUSER")
    DBNAME = os.environ.get("DBNAME")
    DBPASS = os.environ.get("DBPASS")

    engine_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DBUSER,
        password=DBPASS,
        host=DBHOST,
        port=DBPORT,
        database=DBNAME,
    )
    engine = create_engine(url=engine_url)

    Base.metadata.create_all(bind=engine)