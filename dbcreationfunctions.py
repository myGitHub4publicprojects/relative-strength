import sqlite3

conn = sqlite3.connect('/home/jakub/Documents/analizy-finansowe/relative-strength/GPWstocks.db')
conn.text_factory = str
c = conn.cursor()
  
def fromTexttoDB(directory):
    '''directory: string;
    from each text file in a directory extracts data and saves it in a database'''
    import os.path
    # list of files in a directory without temp files:
    list_of_files = [e for e in os.listdir(directory) if not e.endswith('~')]
       
    for e in range(len(list_of_files)):        
        import csv        
        with open('{dir}/{filee}'.format(dir = directory, filee = list_of_files[e]), 'rb') as input_file:
            reader = csv.reader(input_file)
            data = []
            ID = 0
            for row in reader:
                row.insert(0, ID)
                ID += 1
                data.append(row[:-1])
                
        table_name = 'table' + list_of_files[e][:3]   
        with conn:
            c = conn.cursor()
            c.execute("CREATE TABLE {table_name}(ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)".format(table_name = table_name))
            c.executemany("INSERT INTO {table_name} (ID, Date, Open, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?, ?, ?)".format(table_name = table_name), data[1:])      

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