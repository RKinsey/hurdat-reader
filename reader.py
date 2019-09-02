import csv
import sqlite3
#Honestly, BS4 is way too much for this but I've never used it before
from bs4 import BeautifulSoup
from requests import get
from datetime import datetime
from io import StringIO

hurdat_base="https://www.nhc.noaa.gov/data/hurdat/"

class HurdatDB():

    def __init__(self, name="hurdat.db"):
        self.dbconn=sqlite3.connect(name)
        exec_cur=self.dbconn.execute("create table if not exists hurricanes (id TEXT PRIMARY_KEY, name TEXT);")
        self.dbconn.commit()
        exec_cur.execute("""CREATE TABLE IF NOT EXISTS storm_data (
            storm_id TEXT NOT NULL, datestr TEXT NOT NULL, timestr TEXT NOT NULL, status TEXT, lat TEXT, lon TEXT, max_sus_wind INTEGER, min_pressure INTEGER, 
            ne_34 INTEGER, se_34 INTEGER, nw_34 INTEGER, sw_34 INTEGER, ne_50 INTEGER, se_50 INTEGER, nw_50 INTEGER, sw_50 INTEGER, ne_64 INTEGER, se_64 INTEGER, nw_64 INTEGER, sw_64 INTEGER, 
            PRIMARY KEY(storm_id, datestr, timestr),
            FOREIGN KEY(storm_id) REFERENCES hurricanes(id));""")
        self.dbconn.commit()
        exec_cur.close()

    def insert_hurricane(self,storm_id,name,exit=True):
        curs=self.dbconn.execute("insert or replace into hurricane (id, name) values(?,?)",storm_id,name)
        curs.commit()
        if exit is True:
            curs.exit()
        else:
            return curs
         
    def insert_data_entry(self,storm_id, data, exit=True):
        data=[storm_id]+data
        curs=self.dbconn.execute("insert or replace into storm_data values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
        curs.commit()
        if exit is True:
            curs.exit()
        else:
            return curs
        
    def fill_db(self, raw_filename=None):
        raw=StringIO(get_hurdat(),newline='') if raw_filename is None else open(raw_filename,newline='')
        reader=csv.reader(raw)
        curstorm=None
        for r in reader:
            if len(r)==3:
                curstorm=r[0]
                self.insert_hurricane(curstorm,r[1])
            else:
                del(r[2])
                self.insert_data_entry(curstorm,r)
        
    def get_cursor(self):
        return self.dbconn.cursor()

def get_hurdat():
    unparsed=BeautifulSoup(get(hurdat_base).text,'html.parser')
    filename=None
    most_recent=datetime.min
    for e in unparsed.find_all('tr')[3:-1]:
        #print(e)
        #[print("    ", f) for f in e.find_all()]
        fn=e.find("a").get("href")
        if "nepac" in fn or "-" not in fn:
            continue
        dstring=e.find_all("td")[2].text
        post_date=datetime.strptime(dstring,"%Y-%m-%d %H:%M")
        sfn=fn.split("-")
        if post_date>most_recent:
            filename=fn
            most_recent=post_date
    if filename is None:
        raise TypeError
    hurdat_csv=get(hurdat_base+filename).text
    return hurdat_csv


a=HurdatDB()
a.fill_db()
