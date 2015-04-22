# -*- coding: utf-8 -*-
import dataAccessing
def simpleListsPlot(list_of_list):
    ''' takes a list of lists containing tuples, for each list plots values of tuple[1],
    first elements of tuples from first list are used as data labels
    dates must be the same for all items in the list'''
    import pylab
    pylab.figure(1)
    dates = [e[0] for e in list_of_list[0][1:]]
    for e in list_of_list:
        values = [i[1] for i in e[1:]]
        pylab.plot(range(len(values)), values, label = str(e[0]))
    
    pylab.xticks(range(len(dates)), dates, rotation='vertical')
    pylab.xlabel('dates')
    pylab.legend()
    pylab.show()
    
