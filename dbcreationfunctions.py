import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
c = conn.cursor()
conn.text_factory = str

    
def tableCreate(name):
    ''' name: str
    creates table in a database'''
    c.execute("CREATE TABLE {0}(ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)".format(name))

def dataEntry(table_name, ID, Date, Open, High, Low, Close, Volume):
    '''table_name: string;
    ID: int;
    Date: string; in a format 'yearmontday' (e.g. '20010314');
    Open... Volume: floats;'''
    c.execute("INSERT INTO {0} (ID, Date, Open, High, Low, Close, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)".format(table_name), (ID, Date, Open, High, Low, Close, Volume))
    conn.commit()
    
def fromTexttoDB(directory):
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
            
def generateAndInsertSMA(table_name, SMA_period):
    '''table_name: string;
    SMA_period: int
    generates SMA of a given period for a given stock (table_name) and populate
    the table with SMA values,
    if a clumn where data is to be inserted does not existed, function creates it'''
    # check if the column exists, if not, create it
    c.execute("PRAGMA table_info({table})".format(table = 'atc'))
    if 'SMA{period}'.format(period = SMA_period) not in [e[1] for e in c.fetchall()]:
        c.execute("ALTER TABLE {table} ADD COLUMN SMA{period} REAL".format(table = table_name, period = SMA_period))
        
    c.execute("SELECT Date, Close FROM {table}".format(table = table_name))
    date_close = c.fetchall()
    for e in range(SMA_period -1, len(date_close)):
        sum_close = 0
        counter = SMA_period - 1
        for i in range(SMA_period):
            sum_close += date_close[e - counter][1]
            counter -= 1
        sma = sum_close/SMA_period
        c.execute("UPDATE {table} SET SMA{SMA}={sma} WHERE Date={date}".format(table = table_name, SMA = SMA_period, sma = sma, date = date_close[e][0]))
        conn.commit()