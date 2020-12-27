import sqlite3
import time
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
        if getsize(self.filename) < 100: # SQLite DB file header is 100 bytes
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

    def parseIosSMSDB(self):
        # This is an iOS SMS Database
        # To process this we have to:
        #
        # Look in the "sqlite_sequence" table to pull out the table names
        # and max row ids
        # 
        print("Parsing iOS SMS DB file")
        c = self.conn.cursor()
        # msg_tbl = c.execute("select rowid, date_delivered, date_read, text")
        c.execute("select max(rowid) from message")
        max_rowid = c.fetchone()
        c.execute("select seq from sqlite_sequence where name='message'")
        num_rows = c.fetchone()
        num_deleted = num_rows[0] - max_rowid[0]
        print(f"Latest Row ID: {num_rows[0]}")
        print(f"Number of Rows found: {max_rowid[0]}")
        deleted_rows = set()
        for r in range(num_deleted):
            deleted_rows.add(max_rowid[0] + r + 1)
        
        # Now we check the rest of the message table for missing rows
        gaps = self.findGaps('message','rowid')
        if len(gaps) > 0:
            for gap in gaps:
                deleted_rows.add(gap)
        
        # See if we have gaps or missing rows
        if len(deleted_rows) > 0:
            print(sorted(deleted_rows))
        else:
            print("No missing records found")    
        
        
        
        
        
        
        
        
        
        
        # if num_deleted  > 0:
        #     # The last (num_rows[0] - max_rowid[0]) rows of the message table 
        #     # has been deleted
        #     print(f"{num_deleted} row(s) have been deleted from the 'message'"\
        #         " table")
        #     # Find the timestamp of the last available row
        #     c.execute(f"select rowid, date_delivered, date_read from message "\
        #         f"where rowid={max_rowid[0]}")
        #     t = c.fetchone()
        #     # Mac Core Data time stamp is from year 1904 so we have to add 
        #     # 978307200 seconds to it to bring it to the Unix epoch of 1970
        #     dd = time.asctime(time.gmtime( (t[1] / 1000000000) + 978307200 )) \
        #         if t[1] > 0 else "Not Set"
        #     dr = time.asctime(time.gmtime( (t[2] / 1000000000) + 978307200 )) \
        #         if t[2] > 0 else "Not Set"
        
        




    def close(self):
        self.conn.close()


    
