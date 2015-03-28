import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
c = conn.cursor()

    
def tableCreate(name):
    ''' name: str
    creates table in a database'''
    c.execute("CREATE TABLE %s(ID INT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)" % name)

def dataEntry(table_name, ID, Open, High, Low, Close, Volume):
    c.execute("INSERT INTO %s VALUES(%d, %g, %g, %g, %g, %g)" % (table_name, ID, Open, High, Low, Close, Volume))
    conn.commit()