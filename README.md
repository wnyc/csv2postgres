CSV2POSTGRES
============


Intelligently creates schemas for and imports the contents of CSV
files.

    # createdb mydb
    # wget https://github.com/wnyc/csv2postgres/blob/master/examples/tbl_crashes_1995_2009_20111020_sample.csv.gz?raw=true | gzip -dc | http://csv2postgres --connection="db=mydb" --table='tbl_crashes'

Known good datasets
-------------------

* [Transportation Alternative's](http://crashstat.org/about) [geocoded and cleaned crash data](http://crashstat.org/sites/default/files/about/CrashStat3_Ped-Bike-Crashes_1995-2009_20111020.csv.rar)
* [NYC Opendata's](https://data.cityofnewyork.us)
  * [Parking regulation street segment](https://data.cityofnewyork.us/Transportation/Parking-Regulations-Street-Segments/9yzr-h7jq)
  * [Parking regulation sign locations](https://data.cityofnewyork.us/Transportation/Parking-Regulation-Sign-Locations/zibd-yb3i)


