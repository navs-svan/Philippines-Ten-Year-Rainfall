from schema import Base, Places
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from pathlib import Path



if __name__ == "__main__":
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

    csv_file = Path("raw_files/Coordinates/coordinates.csv")

    df = pd.read_csv(csv_file)
    places_list = [
        Places(name=row[0], latitude=row[1], longitude=row[2])
        for row in zip(df["Name"], df["Latitude"], df["Longitude"])
    ]

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    session = Session()
    session.add_all(places_list)
    session.commit()
    session.close()