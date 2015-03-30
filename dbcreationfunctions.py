import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
c = conn.cursor()

    
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
    generate SMA of a given period for a given stock (table_name) and populate
    the table with SMA values'''
    external_current = SMA_period
    while True: 
        c.execute("SELECT Close FROM {table} WHERE ID={ID}".format(table = table_name, ID = external_current))
        if c.fetchone() == None:
            break

        current = external_current
        sum_close = 0
        for a in range(SMA_period):
            c.execute("SELECT Close FROM {table} WHERE ID={ID}".format(table = table_name, ID = current))
            current -= 1
            close = c.fetchone()
            close = close[0]
            sum_close += close

        sma = sum_close/SMA_period
        c.execute("UPDATE {table} SET SMA{SMA}={sma} WHERE ID={ID}".format(table = table_name, SMA = SMA_period, sma = sma, ID = external_current))
        conn.commit()
        external_current += 1