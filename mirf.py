#!python
#
# mirf - A missing record finder for SQLite DBs
#
# Conceptualized by - Shafik Punja (@qubytelogic)
# Coded by - Sheran Gunasekera (@chopstick_)
#
# First release 22nd Dec 2020
# 
# This tool will look through an SQLite Database and find any gaps in
# either primary key based columns or autoincrement columns
#
import os
import sys
import sqlite3
from colorama import Fore, Back, Style, init

def find_gap(sequence):
    indexes = set()
    for idx, num in enumerate(sequence):
        try:
            old_num
        except NameError:
            old_num = num
            continue
        if num - old_num > 1:
            indexes.add(idx)
        old_num = num
    return indexes

def read_db_tables_from(db_file):
    tables = set()
    if not os.path.exists(db_file):
        raise NameError("Can't find specified database file")
    conn = sqlite3.connect(f"file:{db_file}?mode=ro",uri=True)
    c    = conn.cursor()
    results = c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    for result in results:
        tables.add(result[0])
    conn.close()
    return tables

def read_seq_from_table(db_file, table_name, field_name):
    seq = set()
    if not os.path.exists(db_file):
        raise NameError("Can't find specified database file")
    conn = sqlite3.connect(f"file:{db_file}?mode=ro",uri=True)
    c    = conn.cursor()
    results = c.execute(f"SELECT {field_name} FROM {table_name}")
    for result in results:
        seq.add(result[0])
    conn.close()
    return seq

def parse_sqlite_seq(db_file):
    tables = set()
    if not os.path.exists(db_file):
        raise NameError("Can't find specified database file")
    conn = sqlite3.connect(f"file:{db_file}?mode=ro",uri=True)
    c    = conn.cursor()

    # Find all the table names in the sqlite_sequnece table
    results = c.execute(f"SELECT name from sqlite_sequence")
    for result in results:
        tables.add(result[0])
    
    # Open these tables and find the columns that have AUTOINCREMENT
    columns = set()
    print(Style.DIM + "Searching for columns with AUTOINCREMENT in list of tables")
    for table in tables:
        
        results = c.execute(f"SELECT sql from sqlite_master where name='{table}'")
        for result in results:
            
            first_part = result[0].split(',')[0]
            if "AUTOINCREMENT" in first_part:
                columns.add((table,first_part.split('(')[1].split(' ')[0],first_part.split('(')[0].replace("CREATE TABLE ","").strip()))
    
    # Now check for missing sequences in the tables
    for col in columns:
        seq = read_seq_from_table(db_file,col[0],col[1])
        gaps = find_gap(seq)
        if len(gaps) > 0:
            for gap in gaps:
                print(Fore.GREEN + Style.BRIGHT + "[+] Found" + Style.RESET_ALL + f" gap in Row {gap+1} in '{col[0]}'")
                print(f"-- SQL Query that shows gap: SELECT * FROM {col[0]} where {col[1]} in ({gap},{gap+1},{gap+2});")
        else:
            print(Style.DIM + f"No gaps found in '{col[0]}'")
        
    conn.close()
            
# First look for the 'sqlite_sequence' table. If it exists then do auto
# The 'sqlite_sequence' table signifies that there are columns with 
# AUTOINCREMENT specified.
#
# Then read the 'sqlite_sequence' table to determine the columns to lookup

init(autoreset=True)
db_name = input("Enter the full path to the SQLite DB file you want to analyze: [e.g. data/CB_CallHistoryDB/sms.db]  ")

try:
    tables = read_db_tables_from(db_name)
except NameError as e:
    print(e)
    sys.exit(1)

if tables is not None:
    print(Style.BRIGHT + f"Processing file: {db_name}")
    if 'sqlite_sequence' in tables:
        print(Fore.GREEN + Style.BRIGHT + "[+] Found " + Style.RESET_ALL + "'sqlite_sequence' table, parsing")
        parse_sqlite_seq(db_name)
    else:
        print("No 'sqlite_sequence' table finding PRIMARY KEY tables")

