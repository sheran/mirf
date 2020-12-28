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
        ctr = 0
        for idx, num in enumerate(c.fetchall()):
            try:
                old_num
            except NameError:
                old_num = num[0]
            if num[0] - old_num > 1:
                ctr +=1
                indexes.add(idx+ctr)
            old_num = num[0]
        return indexes

    def parseIosSMSDB(self):
        # This is an iOS SMS Database
        # To process this we have to:
        #
        # Look in the "sqlite_sequence" table to pull out the table names
        # and max row ids
        # 
        print("[i] Parsing: " + Fore.GREEN +"iOS SMS DB file")
        c = self.conn.cursor()
        # msg_tbl = c.execute("select rowid, date_delivered, date_read, text")
        c.execute("select max(rowid) from message")
        max_rowid = c.fetchone()
        c.execute("select seq from sqlite_sequence where name='message'")
        num_rows = c.fetchone()
        num_deleted = num_rows[0] - max_rowid[0]
        print(f"[i] Latest Row ID in 'message' table: {num_rows[0]}")
        print(f"[i] Number of Rows found in 'message' table: {max_rowid[0]}")
        deleted_rows = set()
        for r in range(num_deleted):
            deleted_rows.add(max_rowid[0] + r + 1)
        
        # Now we check the rest of the message table for missing rows
        gaps = self.findGaps('message','ROWID')
        if len(gaps) > 0:
            print(f"[i] Found missing records: "+ Fore.GREEN + f"{len(gaps)}")
            for gap in gaps:
                deleted_rows.add(gap)
        

        # See if we have gaps or missing rows
        if len(deleted_rows) > 0:
            # Build the gaps section
            # Iterate over the deleted rows getting records before and
            # after the deleted row
            active_rows = set()
            for row in sorted(deleted_rows):
                row_before = row - 1
                while row_before in deleted_rows:
                    row_before -= 1
                if row_before < 0:
                    row_before = None
                row_after  = row + 1
                while row_after in deleted_rows:
                    row_after += 1
                if row_after > num_rows[0]:
                    row_after = None
                if row_before is not None:
                    active_rows.add(row_before)
                if row_after is not None:
                    active_rows.add(row_after)
            
            selstr = "("
            for k,v in enumerate(active_rows):
                selstr += f"'{v}'"
                if k < len(active_rows) - 1:
                    selstr += ", "
            selstr += ")"

            # fetch data from active rows
            c.execute(f"select ROWID, date, service, text from"\
                f" message where ROWID in {selstr}")
            flat_rows = set()
            for row in c.fetchall():
                dd = time.asctime(time.gmtime( (row[1] / 1000000000) + \
                    978307200 )) if row[1] > 0 else "Not Set"
                msg = row[3]
                if row[2] == "iMessage":
                    msg = "iMessage text"
                if not msg.isascii():
                    msg = ascii(msg).replace("'","")
                else:
                    msg = repr(msg).replace("'","")
                
                flat_rows.add((row[0],dd,msg))
            for row in deleted_rows:
                flat_rows.add((row,"-- missing --",\
                    "-- missing --"))
            self.printGaps(flat_rows)
            
        else:
            print("[i] No missing records found in 'message' table")
        

    #Print out the active and missing rows
    def printGaps(self, flat_rows):
        # Print the table out
        # Col size is: 10, 25, 40 (truncate)
        # hardcoded headers: ROWID, date, text
        print(f"+{'':->10}+{'':->25}+{'':->40}+")
        # print headers
        print(f'|{"ROWID":>10}|{"date (UTC)":>25}|{"text":>40}|')
        print(f"+{'':->10}+{'':->25}+{'':->40}+")
        # print the data rows 
        fl_rows = sorted(flat_rows)

        for row in fl_rows:
            try:
                old_num
            except NameError:
                old_num = row[0]
            if row[0] - old_num > 1:
                print(f'|{".":>10}|{".":>25}|{".":>40.40}|')
                print(f'|{".":>10}|{".":>25}|{".":>40.40}|')
                print(f'|{".":>10}|{".":>25}|{".":>40.40}|')
            print(f'|{row[0]:>10}|{row[1]:>25}|{" "+row[2]:>40.40}|')
            old_num = row[0]
            
        print(f"+{'':->10}+{'':->25}+{'':->40}+")
        
        
    def close(self):
        self.conn.close()


    
