import os
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, NUMERIC
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import URL, create_engine
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Places(Base):
    __tablename__ = "places"

    place_id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(128), unique=True)
    latitude = Column(NUMERIC)
    longitude = Column(NUMERIC)

    place_rainfall = relationship("Rainfall", back_populates="place")


class Dates(Base):
    __tablename__ = "dates"

    date_id = Column(INTEGER, primary_key=True)
    year = Column(INTEGER)
    month = Column(INTEGER)
    day = Column(INTEGER)

    UniqueConstraint(year, month, day, name="date_uix")
    date_rainfall = relationship("Rainfall", back_populates="date")


class Rainfall(Base):
    __tablename__ = "rainfall"

    place_id = Column(INTEGER, ForeignKey(Places.place_id), primary_key=True)
    date_id = Column(INTEGER, ForeignKey(Dates.date_id), primary_key=True)
    amount = Column(NUMERIC)

    UniqueConstraint(place_id, date_id, amount, name="amount_uix")

    def __init__(self, place, date, amount=0):
        self.place = place
        self.date = date
        self.amount = amount

    date = relationship("Dates", back_populates="date_rainfall", lazy="joined")
    place = relationship("Places", back_populates="place_rainfall", lazy="joined")


if __name__ == "__main__":
    # Create the tables
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

    # Populate the Dates table
    Session = sessionmaker(bind=engine)
    session = Session()
    date_list = []
    for year in range(2009, 2020):
        for month in range(1, 13):
            for day in range(1, 32):
                date_list.append(Dates(year=year, month=month, day=day))

    session.add_all(date_list)

    session.commit()
