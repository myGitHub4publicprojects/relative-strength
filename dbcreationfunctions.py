import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
c = conn.cursor()

    
def tableCreate(name):
    ''' name: str
    creates table in a database'''
    c.execute("CREATE TABLE {0}(ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)".format(name))

def dataEntry(table_name, ID, Date, Open, High, Low, Close, Volume):
#add field for SMA
    '''table_name: string;
    ID: int;
    Date: string; in a format 'yearmontday' (e.g. '20010314');
    Open... Volume: floats;'''
    c.execute("INSERT INTO {0} (ID, Date, Open, High, Low, Close, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)".format(table_name), (ID, Date, Open, High, Low, Close, Volume))
    conn.commit()
    
def fromTexttoDB(directory):
#add field for SMA
    '''directory: string;
    from each text file in a directory extracts data and saves it in a database'''
    import os.path
    for filee in os.listdir(directory):
        tableCreate(filee[:3])
        a = open(directory + '/' + filee)
        ID = 0
        for line in a:
            if line.startswith('Date'):
                continue

            ID += 1
            line = line.split(',')
            Date = line[0]
            Open = line[1]
            High = line[2]
            Low = line[3]
            Close = line[4]
            Volume = line[5]
            
            dataEntry(filee[:3], ID, Date, Open, High, Low, Close, Volume)
            
def generateAndInsertSMA(table, SMA_period):
    '''table: string;
    SMA_period: int
    generate SMA of a given period for a given stock (table) and inserts it to the table'''
    c.execute("SELECT Close FROM {table} WHERE ID={ID}".format(table = table, ID = SMA_period))
    date = c.fetchone()
    date = date[0]
    print date