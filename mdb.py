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
        
        self.conn = sqlite3.connect(f"file:{self.filename}", uri=True)
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
        recs = [r[0] for r in c.fetchall()]
        return self.missing_elements(recs)

    def missing_elements(self, L):
        start, end = L[0], L[-1]
        return sorted(set(range(start, end + 1)).difference(L))

    def parseIosCHDB(self):
        # This is an iOS CallHistory Database
        # To process this we have to:
        #
        # Look in the "sqlite_sequence" table to pull out the table names
        # and max row ids
        #
        print()


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
        c.execute("select min(rowid),max(rowid),count(*) from message")
        rows = c.fetchone()
        max_rowid = rows[1]
        min_rowid = rows[0]
        total_rec = rows[2]
        c.execute("select seq from sqlite_sequence where name='message'")
        num_rows = c.fetchone()
        num_deleted = num_rows[0] - max_rowid
        print(f"{'[i] First Record ID:':<26} {min_rowid:>5}")
        print(f"{'[i] Last Record ID:' :<26} {max_rowid:>5}")
        print(f"{'[i] Total Records:' :<26} {total_rec:>5}")
        deleted_rows = set()
        for r in range(num_deleted):
            deleted_rows.add(max_rowid + r + 1)
        
        # Now we check the rest of the message table for missing rows
        gaps = self.findGaps('message','ROWID')
        if len(gaps) > 0:
            print(f"{'[i] Missing Record Count:':<26} {len(gaps):>5}")
            print(f"[i] Missing Records List:\n")
            
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
            c.execute(f"select ROWID, date from"\
                f" message where ROWID in {selstr}")
            flat_rows = set()
            for row in c.fetchall():
                dd = time.asctime(time.gmtime( (row[1] / 1000000000) + \
                    978307200 )) if row[1] > 0 else "Not Set"
                
                flat_rows.add((row[0],dd))
            for row in deleted_rows:
                flat_rows.add((row,"missing"))
            
            
            mrow = []
            start = False
            end = False
            result = []
            line = ""
            for row in sorted(flat_rows):
                #print(row[0])
                if row[1] != "missing" and not start:
                    start = True
                    line += f"between {row[1]} (UTC)"
                if row[1] == "missing" and start and not end:
                    mrow.append(row[0])
                if row[1] != "missing" and start and len(mrow) > 0:
                    end = True
                    start = False
                if end:
                    line = f"{len(mrow)} record(s) missing " + line + \
                        f" and {row[1]} (UTC). Missing record numbers are: " \
                        f"{mrow}"
                    result.append(line)
                    line = ""
                    mrow = []
                    end = False
                
            for i, res in enumerate(result):
                print(f"{i+1}. {res}")

            
        else:
            print("[i] No missing records found in 'message' table")
        

        
    def close(self):
        self.conn.close()


    
