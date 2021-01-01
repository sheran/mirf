#!python
#
# mirf - A missing record finder for SQLite DBs
#
# Inspired by the Windows tool Missing Record Finder
# Developed by Ian Whiffin (@blakdouble)
# More great tools and research on his site: https://www.doubleblak.com/
#
# Conceptualized by - Shafik Punja (@qubytelogic)
# Coded by - Sheran Gunasekera (@chopstick_)
#
# First release 22nd Dec 2020
# 
# This tool will look through an SQLite Database and find any gaps in
# either primary key based columns or autoincrement columns
#


import sys
from colorama import Fore, Back, Style, init

from mdb import SqliteDB



init(autoreset=True)
# Check if command line arguments are given
if len(sys.argv) < 2:
    print(Style.BRIGHT + Fore.RED + "Missing DB file name, please provide a "\
        "filename")
    sys.exit(1)


# Check if the provided arg is a file and is an SQLite DB
db_file = sys.argv[1]
db = SqliteDB(db_file)
try:
    db.open()
except Exception as e:
    print(e)
    sys.exit(1)

if not db.isSQLite3():
    print(Style.BRIGHT + Fore.RED + f"File {db_file} is not an SQLite DB")
    sys.exit(1)

# Check if file is an iOS SMS DB
if db.isSMSDb():
    db.parseIosSMSDB()
    sys.exit(0)
elif db.isCHDb():
    db.parseIosCHDB()
    sys.exit(0)
else:
    print("Starting guided mode:")


# Start Guided mode
# First gather the following data:
# 1) Name of table to check for missing rows
# 2) Name of field in the table from 1 that contains the row count
# 3) [Optional] A table that contains the maximum row count at present



# Get all tables in db
tables = db.readAllTables()
for table in tables:
    print(f"[-] {table}")

table = input(Style.BRIGHT + "Which of the above tables do you want to " \
    "analyze for missing rows? ")

if table not in tables:
    print(Style.BRIGHT + Fore.RED + "Table name selected isn't in DB")
    db.close()
    sys.exit(1)

print(Style.RESET_ALL)
cols = db.getColNames(table)
for col in cols:
    print(f"[-] {col}")

col = input(Style.BRIGHT + "Enter a column to analyze ")
if col not in cols:
    print(Style.BRIGHT + Fore.RED + "Column name selected isn't in table")
    db.close()
    sys.exit(1)

print(Style.RESET_ALL)

if db.getColType(table, col) != "INTEGER":
    print(Style.BRIGHT + Fore.RED + "The column selected is not an INTEGER so "\
        "we cannot analyze")
    db.close()
    sys.exit(1)

gaps = db.findGaps(table,col)
if len(gaps) == 0:
    print(f"No missing entries in the {col} column from the {table} table")
else:
    print(f"Found the following gaps for the {col} column:")
    print(f"Col"+ (len(col)-2) * " " + "ID")
    for gap in gaps:
        print(f"{col} {gap}")
        
    
db.close()

