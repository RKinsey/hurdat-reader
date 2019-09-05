def top_fifty_windspeed(db):
    c=db.get_cursor()
    c.execute("SELECT name,max_sus_wind FROM hurricanes NATURAL JOIN storm_data")


def top_fifty_pressure(db):
