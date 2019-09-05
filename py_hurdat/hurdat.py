import sqlite3
import hurdatdb as rd
import heapq
from joblib import Parallel, delayed
class Hurdat():
    def __init__(self):
        self.db=sqlite3.connect("hurdat.db",factory=rd.HurdatDB,check_same_thread=False)
    def get_by_speed(self, topn=-1):
        hurricanes=self.db.execute("SELECT * FROM hurricanes").fetchall()
        a=Parallel(n_jobs=4)((delayed(self.query_speeds(h[0],True)),h[1]) for h in hurricanes)
        a=sorted(a,reverse=True)
        return a if topn<0 else a[:topn]
    
    def query_speeds(self,storm_id, top=False):
        rows=self.db.execute("SELECT max_sus_wind FROM storm_data WHERE storm_id = ?",storm_id).fetchall()
        if not top:
            return rows
        else:
            top_speed=-100 #entries without data are -99 in hurdat
            for row in rows:
                if row[0]>top_speed:
                    top_speed=row[0]
            return top_speed

print(Hurdat().get_by_speed())
