import sqlite3
import sys
from colorama import Fore, Back, Style, init

class SqliteDB:
    def __init__(self, filename):
        init(autoreset=True)
        self.filename = filename    
        self.all_tables = None

    def isSQLite3(self):
        from os.path import isfile, getsize

        if not isfile(self.filename):
            return False
        if getsize(self.filename) < 100: # SQLite database file header is 100 bytes
            return False

        with open(self.filename, 'rb') as fd:
            header = fd.read(100)

        return header[:16] == b'SQLite format 3\x00'
    
    def open(self):
        if not self.isSQLite3():
            raise sqlite3.DatabaseError("Not an SQLite DB")
        
        self.conn = sqlite3.connect(f"file:{self.filename}?mode=ro", uri=True)
        self.test()
    
    def test(self):
        c = self.conn.cursor()
        c.execute("select name from sqlite_master where type='table'")


    # Is the file an iPhone SMS database?
    #
    # Characteristics:
    #
    # 1) Must have an 'sqlite_sequence' table
    # 2) 'sqlite_sequence' table should have: (chat, handle, message, attachment
    # tables
    def isSMSDb(self):
        c    = self.conn.cursor()
        c.execute('SELECT name from sqlite_master where name="sqlite_sequence"')    
        if c.fetchone() is None:
            return False
        
        sig   = {'chat','handle','message','attachment'}
        tables = set()
        c.execute('SELECT name from sqlite_sequence')
        for row in c.fetchall():
            tables.add(row[0])
        
        # We look in the list of tables in sqlite_sequence to see if it contains
        # all the table names in sig. If so, we consifer it a match.
        return sig.issubset(tables)
    
    # Is the file an iPhone Call History database?
    #
    # Characteristics:
    #
    # 1) Must have ('ZCALLDBPROPERTIES','ZCALLRECORD','Z_PRIMARYKEY') tables
    def isCHDb(self):
        c    = self.conn.cursor()
        sig   = {'ZCALLDBPROPERTIES','ZCALLRECORD','Z_PRIMARYKEY'}
        tables = set()
        c.execute('SELECT name from sqlite_master where type="table"')
        for row in c.fetchall():
            tables.add(row[0])
        
        # We look in the list of tables in sqlite_sequence to see if it contains
        # all the table names in sig. If so, we consifer it a match.
        return sig.issubset(tables)
    
    def readAllTables(self,force=False):
        if self.all_tables is None or force:
            c    = self.conn.cursor()
            tables = set()
            c.execute('SELECT name from sqlite_master where type="table"')
            for row in c.fetchall():
                tables.add(row[0])
            self.all_tables = tables
            return tables
        else:
            return self.all_tables
    
    def readColsFrom(self, table):
        c    = self.conn.cursor()
        cols = set()
        c.execute(f"pragma table_info({table})")
        for row in c.fetchall():
            cols.add(row)
        return cols
    
    def getColNames(self, table):
        cols = self.readColsFrom(table)
        return {col[1] for col in cols}

    def getColType(self, table, col):
        cols = self.readColsFrom(table)
        for c in cols:
            if col == c[1]:
                return c[2]

    def findGaps(self, table, col):
        c    = self.conn.cursor()
        c.execute(f"SELECT {col} from {table}")
        indexes = set()
        for idx, num in enumerate(c.fetchall()):
            try:
                old_num
            except NameError:
                old_num = num[0]
                continue
            if num[0] - old_num > 1:
                indexes.add(idx)
            old_num = num[0]
        return indexes

    # def parseIosSMSDB(self):
    #     # This is an iOS SMS Database
    #     # To process this we have to:
    #     #
    #     # Look in the "sqlite_sequence" table to pull out the table names
    #     # and max row ids
    #     #
    #     # 
    #     c = self.conn.cursor()
    #     ss_tables = c.execute("")


    def close(self):
        self.conn.close()


    
