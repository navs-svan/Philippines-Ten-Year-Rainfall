import pandas as pd
from pathlib import Path
from schema import Base, Places, Dates, Rainfall
from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import sessionmaker
import os


def func(row, session, year, month):
    rainfalls = []
    amounts = [col for col in row]
    station = row.name

    place_stmt = select(Places).where(Places.name == station)
    place_obj = session.execute(place_stmt).first()[0]

    for amount, day in zip(amounts, row.index):
        date_stmt = select(Dates).where(
            Dates.year == year, Dates.month == month, Dates.day == day
        )
        date_obj = session.execute(date_stmt).first()[0]

        rainfalls.append(Rainfall(place=place_obj, date=date_obj, amount=amount))

    session.add_all(rainfalls)
    session.commit()

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
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    root_dir = Path(__file__).resolve().parent.parent
    filepath = root_dir / "raw_files" / "PAG-ASA" / "Summarized_Rainfall_Data.xlsx"

    params = {"sheet_name": "Sheet1", "nrows": 56, "header": [0, 1, 2], "index_col": 0}

    df = pd.read_excel(filepath, **params)

    idx = pd.IndexSlice
    for year in range(2009, 2020):
        for month in range(1, 13):
            A = df.loc[:, idx[year, month]]
            A.apply(func, axis=1, args=(session, year, month))
    
    session.close()
    
