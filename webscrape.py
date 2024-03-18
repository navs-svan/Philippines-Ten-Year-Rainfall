import requests
from bs4 import BeautifulSoup
import csv

url = "https://www.philatlas.com/municipalities.html"

r = requests.get(url=url)

if r.status_code == 200:
    soup = BeautifulSoup(r.text, 'lxml')
    brgy_table = soup.find_all("ul", id="brgyList")
    brgy_list = brgy_table[0].find_all("li")


    with open("municipality.csv", 'w', newline='') as f:
        writer =  csv.writer(f, delimiter=',')
        for brgy in brgy_list:
            writer.writerow(brgy.text.split(', '))
        

else:
    print(f"Error occurred: {r.status_code}")

