import sqlite3
from html import unescape

import psycopg2
import requests
from bs4 import BeautifulSoup
import datetime
from nltk.corpus import stopwords
import string




SCHEDULE_YEAR = 1950
IS_DEV_MODE = True

days = ['sunday', 'monday','tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
con = psycopg2.connect(database="Time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.149", port=5432)
cur = con.cursor()

while SCHEDULE_YEAR < 1990:
    URL = f"https://classic-tv.com/features/past-tv-schedules/{SCHEDULE_YEAR}-{SCHEDULE_YEAR+1}-tv-schedule"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    parent = soup.find("div", class_="itemFullText")
    tables = parent.find_all("table")
    day_counter = 0
    # ABC,I Love Lucy,20:00,Monday,1
    for table in tables:
        rows = table.find_all("tr")

        t = rows.pop(0).find_all('th')[1].text.replace('PM','').strip()

        for row in rows:
            d = datetime.datetime.strptime(t, '%H:%M')
            network = row.find('th').text
            cells = row.find_all("td")
            print(days[day_counter])
            for cell in cells:
                if "colspan" in cell.attrs:
                    show_length = int(cell.attrs['colspan']) * 30
                else:
                    show_length = 30

                show_title = cell.text.replace("\u2019", "'").replace("\u2013", "-").replace("\u2018", "'").replace("\u2014", "-")
                values_string = tuple()
                insert_query = f'INSERT INTO schedules (network, show_title, air_date, air_time, day_of_week) VALUES (%s,%s,%s,%s,%s);'
                cur.execute(insert_query, (network, unescape(show_title), SCHEDULE_YEAR, d.strftime("%I:%M"), days[day_counter]))
                con.commit()
                print(network, show_title, d.strftime("%I:%M"), days[day_counter], SCHEDULE_YEAR)
                d = d + datetime.timedelta(minutes=show_length)
        day_counter +=1
    SCHEDULE_YEAR += 1