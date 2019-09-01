import csv
import sqlite3
#Honestly, BS4 is way too much for this but I've never used it before
from bs4 import BeautifulSoup
from requests import get
from datetime import datetime
from io import StringIO

hurdat_base="https://www.nhc.noaa.gov/data/hurdat/"

class HurdatDB():
    def __init__(self):
        self.dbconn=sqlite3.connect("hurdat.db")
        self.dbconn.executescript("""create table if not exists hurricanes (id TEXT PRIMARY_KEY, name TEXT);
        create table if not exists records (datestr TEXT, timestr TEXT, record TEXT, status TEXT, lat TEXT, long TEXT, 
        max_sus_wind INTEGER, min_pressure INTEGER, 34_ne, 34_se, 34_nw, 34_sw, 50_ne,50_se,50_nw,50_sw,64_ne,64_se,64_nw,64_sw);""").close()

    def insert_hurricane(id,name):
         

    def make_table(self,name,):
        c=self.dbconn.cursor()
        c.execute("CREATE TABLE ? (")

class HurdatReader():
    def __init__(self):
        self.header_format="id","name","entries"
        self.data_format="datestr","timestr","record","status","lat","long","max_sus","min_press","34ne","34se","34nw","34sw","50ne","50se","50nw","50sw","64ne","64se","64nw","64sw"

    def __init__(self):
        self._raw=get_hurdat()
        self.hurdat=parse_hurdat()
    @staticmethod
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

    
    def parse_hurdat(self,hurdat=None):
        if hurdat is None:
            hurdat=self._raw
        reader=csv.reader(StringIO(hurdat,newline=''))
        curstorm=None
        for r in hurdat:
            if len(r)==3:
                curstorm={"id":r[0],"name":r[1]}

        parsed=None
        return parsed

