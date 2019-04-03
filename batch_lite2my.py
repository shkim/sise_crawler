import re
import glob
import sqlite_to_mysql

re_dbfile = re.compile(r'^daysise_(\d+)\.sqlite$')

for filename in glob.glob("daysise_*.sqlite"):
    m = re_dbfile.match(filename)
    if not m:
        print('Invalid DB filename:', filename)
        continue

    sqlite_to_mysql.copy_sqlite_to_mysql(m[1], 'config.ini', False)

print("Batch done")
