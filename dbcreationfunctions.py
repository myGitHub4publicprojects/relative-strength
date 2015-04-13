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
      
def generateSMA(SMA_period, table_name):
    ''' SMA_period: integer,
    table_name: string, name of a table in db.
    return a list of SMA values, the list lenght is equal to numer of rows in a table
    in the first rows where SMA cannot be calculated None is inserted
    '''
    c.execute("SELECT Close FROM {table}".format(table = table_name))
    price_at_close = c.fetchall()
    
    close_list = []
    
    for e in range(SMA_period -1, len(price_at_close)):
        sum_close = 0
        counter = SMA_period - 1
        for i in range(SMA_period):
            sum_close += price_at_close[e - counter][0]
            counter -= 1
        sma = sum_close/SMA_period
        close_list.append(sma)
    close_list = (SMA_period - 1) * [None] + close_list
    return close_list
    
def tablesWithDataandSMA(table_name):
    ''' generates table with following fields:
        ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL
        SMA 2 to 20, from 20 to 100 every 5, from 100 to 200 every 10, 200, 250 and 300
    '''
    # list of SMAs for w given stock
# change next line
    list_of_smas = range(2, 10) # + range(20, 100, 5) + range(100, 200, 10) + [200, 250, 300]
    
    #append each list of SMA values to list_of_smas_values
    list_of_smas_values = []
    for e in list_of_smas:
        list_of_smas_values.append(generateSMA(e, table_name))
        
    # list of lists of SMAs for each day
    smas_by_days = []
    for e in range(len(list_of_smas_values[0])):
        same_day_smas = []
        for i in range(len(list_of_smas_values)):
            same_day_smas.append(list_of_smas_values[i][e])
        smas_by_days.append(same_day_smas)
        
    # get data from table_name in a form of list of lists (individual records)
    with conn:
        c = conn.cursor()
        c.execute("SELECT * FROM {table}".format(table = table_name))
        data = [list(e) for e in c.fetchall()]
     
    # append SMAs at the end of each record in data
    extended_data = []
    for e in range(len(data)):
        extended_data.append(data[e] + smas_by_days[e])
           
    # create new table -> 'table_name + withSMAs' and inserts data from table_name
    # and SMAs (extended_data)
    smas = ','.join([' SMA' + str(e) + ' REAL' for e in list_of_smas])
    smas2 = ','.join([' SMA' + str(e) for e in list_of_smas])
    questmarks = '?' + (', ?') * (6 + len(list_of_smas))
    with conn:
        c = conn.cursor()
        c.execute("CREATE TABLE {new_table} (ID INTEGER PRIMARY KEY, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL, {smas})".format(new_table = table_name + 'withSMAs', smas = smas))
        c.executemany("INSERT INTO {new_table} (ID, Date, Open, High, Low, Close, Volume, {smas2}) VALUES ({questmarks})".format(new_table = table_name + 'withSMAs', smas2 = smas2, questmarks = questmarks), extended_data)

 