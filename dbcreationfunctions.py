import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
conn.text_factory = str
c = conn.cursor()
    
def tableCreate(name):
    ''' name: str
    creates table in a database'''
    c.execute("CREATE TABLE {0}(ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)".format(name))
    
def fromTexttoDB(directory):
    '''directory: string;
    from each text file in a directory extracts data and saves it in a database'''
    import os.path
    list_of_files = os.listdir(directory)
    last_started = None
    
    #   removes table that was started in previous run and might not be filled completely
    for filee in list_of_files:
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ('table' + filee[:3],))
        fetched = c.fetchone()
        if fetched != None:
            last_started = fetched[0] + '.txt'
        if fetched == None:
            if last_started != None:
                c.execute("DROP TABLE {table_name}".format(table_name = last_started[:-4]))
            break
    
    # create and fill tables starting with first file or one that corresponds to
    # the table that was removed by previuos block                
    if last_started == None:
        last_started = 'table' + list_of_files[0]  
              
    for e in range(list_of_files.index(last_started[5:]), len(list_of_files)):
        
        table_name = 'table' + list_of_files[e][:3]    
        tableCreate(table_name)
        print list_of_files[e]
    
        import csv        
        with open('/home/jakub/Documents/analizy-finansowe/wse stocks/{filee}'.format(filee = list_of_files[e]), 'rb') as input_file:
            reader = csv.reader(input_file)
            data = []
            ID = 0
            for row in reader:
                row.insert(0, ID)
                ID += 1
                data.append(row[:-1])
        c.executemany("INSERT INTO {table_name} (ID, Date, Open, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?, ?, ?)".format(table_name = 'table' + list_of_files[e][:3]), data[1:])
        conn.commit()
            
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