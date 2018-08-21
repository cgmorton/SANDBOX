#!/usr/bin/env python

import json
from scipy.stats import linregress
from scipy.stats.stats import pearsonr


def correlate_two(var_name, rcp, data_dir):
    with open(data_dir + var_name + '_' + rcp + '_ts_years.json') as f:
        var_ts_data = json.load(f)
    with open(data_dir + var_name + '_' + rcp + '_ind_sums_years.json') as f:
        index_ts_data = json.load(f)
    #return pearsonr(var_ts_data, index_ts_data)
    #  slope, intercept, r_value, p_value, std_err
    return linregress(var_ts_data, index_ts_data)


########
#M A I N
########
if __name__ == '__main__' :
     LOCA_CMIP5_MODELS = {
        #'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        'CMCC-CMS':[1950,2100],
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    years = range(1951,2012)
    for model in LOCA_CMIP5_MODELS.keys():
        print('PROCESSING MODEL ' +model)
        data_dir = '/media/DataSets/loca/' + model + '/'
        print('REGRESSION 2')
        for var_name in ['tmin', 'tmax']:
            for rcp in ['rcp45', 'rcp85']:
                print var_name, rcp
                reg = correlate_two(var_name, rcp, data_dir)
                # p_val, r_val, interc, slope
                print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))
        print('REGRESSION SEPARATE')
        for var_name in ['tmin', 'tmax']:
            for rcp in ['rcp45', 'rcp85']:
                print var_name, rcp
                # Indices
                with open(data_dir + var_name + '_rcp85' + '_ind_sums_years.json') as f:
                    index_ts_data = json.load(f)
                    reg_data = [range(len(index_ts_data)), index_ts_data]
                    reg = linregress(reg_data)
                    print ('Index Regression')
                    # p_val, coeff, intercept, slope
                    print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))
                # Temps
                with open(data_dir + var_name + '_rcp85' + '_ts_years.json') as f:
                    var_ts_data = json.load(f)
                    reg_data = [range(len(var_ts_data)), var_ts_data]
                    reg = linregress(reg_data)
                    print ('TEMP Regression')
                    # p_val, coeff, intercept, slope
                    print(str(reg[3]) + ',' +  str(reg[2]) + ',' +  str(reg[1]) + ',' + str(reg[0]))
