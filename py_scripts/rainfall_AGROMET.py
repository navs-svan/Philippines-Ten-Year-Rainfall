import pandas as pd
from pathlib import Path
from schema import Base, Places, Dates, Rainfall
from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import os
import re


def insert_amount(row, session, year, month):
    amounts = [col for col in row]
    station = row.name

    place_stmt = select(Places).where(Places.name == station)
    place_id = session.execute(place_stmt).first()[0].place_id

    for amount, day in zip(amounts, row.index):
        date_stmt = select(Dates).where(
            Dates.year == year, Dates.month == month, Dates.day == day
        )
        date_id = session.execute(date_stmt).first()[0].date_id

        details = {"place_id": place_id,
                  "date_id": date_id,
                  "amount": amount,
                  }
        
        session.execute(insert(Rainfall).values(details).on_conflict_do_nothing())

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
    csv_dir = root_dir / "raw_files" / "Agromet"

    pattern = re.compile(r"Rainfall\sAnalysis-(.*)\s[0-9]{4}\.csv")
    month_map_dict = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }
    params = {"skiprows": 1, "index_col": 1}
    days = [str(i) for i in range(1, 32)]

    year_folders = Path(csv_dir).glob("*")
    for folder in year_folders:
        year = folder.name
        csv_files = folder.glob("*.csv")
        for f in csv_files:
            month = re.search(pattern, f.name)
            month = month_map_dict[month.group(1)]

            df = pd.read_csv(f, **params)
            df = df[df.columns.intersection(days)]
            df.apply(insert_amount, axis=1, args=(session, year, month))

    session.close()
   
 
