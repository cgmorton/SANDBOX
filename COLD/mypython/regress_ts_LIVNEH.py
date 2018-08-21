#!/usr/bin/env python

import json
from scipy.stats import linregress
from scipy.stats.stats import pearsonr


def correlate_two(var_name, data_dir):
    print 'VAR ' + var_name
    with open(data_dir + var_name + '_ts_years.json') as f:
        var_ts_data = json.load(f)
    with open(data_dir + var_name + '_ind_sums_years.json') as f:
        index_ts_data = json.load(f)
    #return pearsonr(index_ts_data, var_ts_data)
    return linregress(var_ts_data, index_ts_data)

########
#M A I N
########
if __name__ == '__main__' :
    years = range(1951,2012)
    #years = range(1951, 1953)
    data_dir = 'RESULTS/livneh/'

    for var_name in ['tmin', 'tmax']:
        #slope, intercept, rvalue, pvalue, stderr
        reg = correlate_two(var_name, data_dir)
        print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))

    '''
    for var_name in ['tmin', 'tmax']:
        print('VARIABLE ' + var_name)
        # Indices
        with open(data_dir + var_name + '_ind_sums_years.json') as f:
            index_ts_data = json.load(f)
            reg_data = [range(len(index_ts_data)), index_ts_data]
            reg = linregress(reg_data)
            print ('Index Regression')
            # p_val, coeff, intercept, slope
            print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))
        # Temps
        with open(data_dir + var_name + '_ts_years.json') as f:
            var_ts_data = json.load(f)
            reg_data = [range(len(var_ts_data)), var_ts_data]
            reg = linregress(reg_data)
            print ('TEMP Regression')
            # p_val, coeff, intercept, slope
            print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))
    '''
