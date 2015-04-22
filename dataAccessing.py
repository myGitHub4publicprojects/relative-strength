from dbcreationfunctions import *
def closePrice(stock, date):
    ''' stock: string (3 characters), date: string (e.g. '20101204')
    returns close price of the stock in a given day,
    if given date is before stock debuted on GPW returns 'use later date'
    if no data at a given day return 'no trading at this date' '''
    table_name = 'table' + stock + 'withSMAs'
    c.execute("SELECT Close FROM {table_name} WHERE Date=?".format(table_name = table_name), (date,))
    result = c.fetchone()
    
    if result == None:
        c.execute("SELECT Date FROM {table_name} WHERE ID=1".format(table_name = table_name))
        first_date = c.fetchone()[0]
        if first_date > date:
            return 'use later date'
        else:
            return 'no trading at this date'        
    
    return result
    
def pricesInRange(stock, date1, date2):
    ''' stock: string (3 characters), date1: string (e.g. '20101204'), date2: string
    returns a list of tuples, first tuple is (stock, SMA X) X - period,
    nest tuples are date and close price of a given stock.
    if date1 > date2 returns: 'date1 cannot be after date2'
    if date2 is before first date in a table returns: 'use later date at least for date2'
    '''
    # data1 cannot be after data2
    if date1 > date2:
        return 'date1 cannot be after date2'
    
    # data 2 must be in a table
    table_name = 'table' + stock + 'withSMAs'
    c.execute("SELECT Date FROM {table_name} WHERE ID=1".format(table_name = table_name))
    first_date = c.fetchone()[0]
    if date2 < first_date:
        return 'use later date at least for date2'
    
    
    c.execute("SELECT Date, Close FROM {table_name} WHERE Date>=? AND Date<=?".format(table_name = table_name), (date1, date2))
    result = c.fetchall()
    result.insert(0,(stock, 'close price'))
    return result

def SMAatDate(stock, SMA_period, date):
    ''' stock: string (3 characters), SMA_period: integer; date: string (e.g. '20101204')
    returns SMA value of the stock in a given day,
    if given date is before stock debuted on GPW returns 'use later date'
    if no data at a given day return 'no trading at this date' '''
    
    table_name = 'table' + stock + 'withSMAs'
    c.execute("SELECT SMA{SMA_period} FROM {table_name} WHERE Date=?".format(SMA_period = SMA_period, table_name = table_name), (date,))
    SMAvalue = c.fetchone()
    if SMAvalue != None:
        return SMAvalue
    else:
        return 'no data for this date'
    #add some code to output if if was a weekend or a date is from the period before stock debuted on GPW

def SMAinRange(stock, SMA_period, date1, date2):
    ''' stock: string (3 characters), SMA_period: int; date1: string (e.g. '20101204'), date2: string
    returns a list of tuples, first tuple is (stock, SMA X) X - period, 
    other tuples are date and close price of a given stock
    if date1 > date2 returns: 'date1 cannot be after date2' ''' 
    # data1 cannot be after data2
    if date1 > date2:
        return 'date1 cannot be after date2'
        
    table_name = 'table' + stock + 'withSMAs'
    c.execute("SELECT Date, SMA{SMA_period} FROM {table_name} WHERE Date>=? AND Date<=?".format(SMA_period = SMA_period, table_name = table_name), (date1, date2))
    result = c.fetchall()
    result.insert(0,(stock, 'SMA' + str(SMA_period)))
    return result