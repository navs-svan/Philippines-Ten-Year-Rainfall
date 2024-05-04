import requests
from bs4 import BeautifulSoup
import re
import os
from pdf2image import convert_from_path
import pytesseract
from schema import Base, Places
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert


def extract_text(image):
    text = pytesseract.image_to_string(image)
    return text


def insert_data(engine, details_dict):

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    session = Session()

    session.execute(insert(Places).values(details_dict).on_conflict_do_nothing())
    session.commit()


def convert_decimal(coordinate, regex_patterns):
    degrees = re.search(regex_patterns["degrees"], coordinate)
    minutes = re.search(regex_patterns["minutes"], coordinate)
    seconds = re.search(regex_patterns["seconds"], coordinate)

    dd = (
        float(degrees.group(1))
        + float(minutes.group(1)) / 60
        + float(seconds.group(1)) / 3600
    )
    return round(dd, 6)


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

    patterns = {
        "longitude": re.compile(r"LONGITUDE: (.*)"),
        "latitude": re.compile(r"LATITUDE: (.*)"),
        "station": re.compile(r"STATION: (.*) LATITUDE"),
        "degrees": re.compile(r"^([0-9]*)"),
        "minutes": re.compile(r"([0-9]*)'"),
        "seconds": re.compile(r"([0-9\.]+)[EN\"]"),
    }

    url = "https://www.pagasa.dost.gov.ph/climate/climate-data"
    r = requests.get(url=url, verify=False)

    pdf_file = "py_scripts/station.pdf"

    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "lxml")
        station_details = soup.select("div#climate-climatological-normals td a")

        for station_detail in station_details:
            url = station_detail.get("href")
            response = requests.get(url)
            with open(pdf_file, "wb") as f:
                f.write(response.content)

            pages = convert_from_path(pdf_file)
            text = ""
            for page in pages:
                text += extract_text(page)

            longitude = re.search(patterns["longitude"], text)
            latitude = re.search(patterns["latitude"], text)
            station = re.search(patterns["station"], text)

            d_latitude = convert_decimal(latitude.group(1), patterns)
            d_longitude = convert_decimal(longitude.group(1), patterns)

            details = {
                "name": station.group(1),
                "latitude": d_latitude,
                "longitude": d_longitude,
            }

            insert_data(engine, details)

    else:
        print(f"Error occurred: {r.status_code}")

    os.remove(pdf_file)
