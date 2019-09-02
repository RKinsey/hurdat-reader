import csv
import sqlite3
import os
#Honestly, BS4 is way too much for this but I've never used it before
from bs4 import BeautifulSoup
from requests import get
from datetime import datetime
from io import StringIO

hurdat_base="https://www.nhc.noaa.gov/data/hurdat/"

class HurdatDB():

    def __init__(self, name="hurdat.db", fill=False):
        premade=True if os.path.isfile(name) else False
        self.dbconn=sqlite3.connect(name)
        if not premade:
            exec_cur=self.dbconn.executescript(""" 
                CREATE TABLE IF NOT EXISTS hurricanes (id TEXT PRIMARY_KEY, name TEXT);
                CREATE TABLE IF NOT EXISTS storm_data (
                    storm_id TEXT NOT NULL, datestr TEXT NOT NULL, timestr TEXT NOT NULL, status TEXT, lat TEXT, lon TEXT, max_sus_wind INTEGER, min_pressure INTEGER, 
                    ne_34 INTEGER, se_34 INTEGER, nw_34 INTEGER, sw_34 INTEGER, ne_50 INTEGER, se_50 INTEGER, nw_50 INTEGER, sw_50 INTEGER, ne_64 INTEGER, se_64 INTEGER, nw_64 INTEGER, sw_64 INTEGER, 
                    PRIMARY KEY(storm_id, datestr, timestr),
                    FOREIGN KEY(storm_id) REFERENCES hurricanes(id));
                """)
            self.dbconn.commit()
            exec_cur.close()
        if fill:
            self.fill_db()
    
    def __del__(self):
        self.dbconn.close()

    def insert_hurricane(self,storm_id,name,do_commit=True):
        hctup=storm_id,name
        curs=self.dbconn.execute("insert or replace into hurricanes (id, name) values(?,?)",hctup)
        if do_commit is True:
            self.dbconn.commit()

         
    def insert_data_entry(self,storm_id, data,do_commit=True):
        data=[storm_id]+data
        curs=self.dbconn.execute("insert or replace into storm_data values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
        if do_commit is True:
            self.dbconn.commit()
        return curs
        
    def fill_db(self, raw_filename=None):
        raw=StringIO(get_hurdat(),newline='') if raw_filename is None else open(raw_filename,newline='')
        reader=csv.reader(raw)
        curstorm=None
        for r in reader:
            del(r[-1])
            r=[e.strip() for e in r]
            if len(r)==3:
                curstorm=r[0]
                self.insert_hurricane(curstorm,r[1],False)
            else:
                del(r[2])
                self.insert_data_entry(curstorm,r,False)
        self.dbconn.commit()

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
        dstring=e.find_all("td")[2].text.strip()
        try:
            post_date=datetime.strptime(dstring,"%Y-%m-%d %H:%M")
        except:
            #sometimes requests.get returns the date in the wrong format, so this should handle those times
            post_date=datetime.strptime(dstring,"%d-%b-%Y %H:%M")
        sfn=fn.split("-")
        if post_date>most_recent:
            filename=fn
            most_recent=post_date
    if filename is None:
        raise TypeError
    hurdat_csv=get(hurdat_base+filename).text
    return hurdat_csv
