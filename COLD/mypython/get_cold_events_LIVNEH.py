import sys
import numpy as np
import json
from netCDF4 import Dataset

def compute_mon_day(doy):
    '''
    Reverse of compute_doy but counting every feb as having 29 days
    '''
    ndoy = int(doy)
    mon = 0
    day = 0
    if ndoy >366 or ndoy < 1:
        return None,None
    mon_day_sum = [31,60,91,121,152,182,213,244,274,305,335,366]
    for i in range(12):
        if i == 0:
            if ndoy <=31:
                mon = 1
                day = ndoy
                break
        else:
            if mon_day_sum[i-1] < ndoy and ndoy <= mon_day_sum[i]:
                mon = i+1
                day = ndoy - mon_day_sum[i-1]
                break
            else:
                continue
    return mon,day

def get_cold_days_from_pca_1(var_name, data_dir):
    with open(data_dir + var_name + '_pca_component_1_ts.json') as f:
        ts_data = json.load(f)
    dates = []
    vals = []
    for d_idx, date_val in enumerate(ts_data):
        '''
        d_int = date_val[0]
        year = 1950 + d_int / 90
        doy = d_int % 90
        mon, day = compute_mon_day(doy)
        mon = str(mon); day = str(day)
        if len(mon) == 1: mon = '0' + mon
        if len(day) == 1: day = '0' + day
        dates.append(str(year) + '-' + mon + '-' + day)
        '''
        dates.append(date_val[0])
        vals.append(date_val[1])
    np_vals = np.array(vals)
    #find 10 coldest days
    max_idx = list(np.argsort(np_vals)[-10:])
    max_dates = []
    for m_idx in max_idx:
        max_dates.append(dates[m_idx])
    return max_dates

def get_cold_days_from_index(var_name, years, data_dir):
    dates = []
    max_indices = []
    for year_idx, year in enumerate(years):
        print 'YEAR ' + str(year)
        net_file = data_dir + var_name + '_5th_Indices_WUSA_' + str(year) + '.nc'
        ds = Dataset(net_file, 'r')
        indices = ds.variables['index'][:,:,:]
        count = 1
        # find 10 largest  value in each year
        while count <= 10:
            doy_idx,j,k = np.unravel_index(indices.argmax(), indices.shape)
            val = indices[doy_idx,j,k]
            #print vals
            if doy_idx <= 30:
                doy = doy_idx + 334 + 1
            else:
                doy = doy_idx - 31 + 1
            mon, day = compute_mon_day(doy)
            mon = str(mon); day = str(day)
            if len(mon) == 1: mon = '0' + mon
            if len(day) == 1: day = '0' + day
            dates.append(str(year) + '-' + str(mon) + '-' + str(day))
            max_indices.append(val)
            # take that date out for next round
            indices[doy_idx,:,:] = 0
            count+=1
    # Find 10 max values overall
    ten_idx = np.array(max_indices).argsort()[-10:][::-1]
    ten_dates = []
    for t_idx in ten_idx:
        ten_dates.append(dates[int(t_idx)])
    return ten_dates

def get_cold_days_from_index_all_domain(var_name, years, data_dir):
    dates_sums = []
    max_indices_sums = []
    dates_aves = []
    max_indices_aves = []
    for year_idx, year in enumerate(years):
        print 'YEAR ' + str(year)
        net_file = data_dir + var_name + '_5th_Indices_WUSA_' + str(year) + '.nc'
        ds = Dataset(net_file, 'r')
        num_lats = ds.variables['lat'][:].shape[0]
        num_lons = ds.variables['lon'][:].shape[0]
        indices = ds.variables['index'][:,:,:]
        count = 1
        ind_sums = []
        ind_aves = []
        for doy_idx in range(90):
            ind_sums.append(sum(indices[doy_idx].reshape(num_lats*num_lons)))
            ind_aves.append(np.mean(indices[doy_idx].reshape(num_lats*num_lons)))
        ind_sums = np.array(ind_sums)
        ind_aves = np.array(ind_aves)
        # find 10 largest value of sums in each year
        doy_idxs = ind_sums.argsort()[::-1][:10]
        for doy_idx in doy_idxs:
            doy = doy_idx + 1
            if doy_idx <= 30:
                doy = doy_idx + 334 + 1
            else:
                doy = doy_idx - 31 + 1
            mon, day = compute_mon_day(doy)
            mon = str(mon); day = str(day)
            if len(mon) == 1: mon = '0' + mon
            if len(day) == 1: day = '0' + day
            dates_sums.append(str(year) + '-' + str(mon) + '-' + str(day))
            max_indices_sums.append(ind_sums[doy_idx])
        # find 10 largest value of aves in each year
        doy_idxs = ind_aves.argsort()[::-1][:10]
        for doy_idx in doy_idxs:
            doy = doy_idx + 1
            if doy_idx <= 30:
                doy = doy_idx + 334 + 1
            else:
                doy = doy_idx - 31 + 1
            mon, day = compute_mon_day(doy)
            mon = str(mon); day = str(day)
            if len(mon) == 1: mon = '0' + mon
            if len(day) == 1: day = '0' + day
            dates_aves.append(str(year) + '-' + str(mon) + '-' + str(day))
            max_indices_aves.append(ind_sums[doy_idx])

    # Find 10 max sum values overall
    ten_idx_sums = list(np.array(max_indices_sums).argsort()[-10:][::-1])
    ten_dates_sums = []
    years_in = []
    while len(ten_dates_sums) < 10:
        for t_idx in ten_idx_sums:
            if dates_sums[int(t_idx)][0:4] not in years_in:
                ten_dates_sums.append(dates_sums[int(t_idx)])
                years_in.append(dates_sums[int(t_idx)][0:4])
            else:
                ind = dates_sums.index(dates_sums[int(t_idx)])
                del max_indices_sums[ind]
                del dates_sums[ind]
                ten_idx_sums = list(np.array(max_indices_sums).argsort()[-10:][::-1])
        if len(ten_dates_sums) == 10: break

    # Find 10 max aves values overall
    ten_idx_aves = list(np.array(max_indices_aves).argsort()[-10:][::-1])
    ten_dates_aves = []
    years_in = []
    while len(ten_dates_aves) < 10:
        for t_idx in ten_idx_aves:
            if dates_aves[int(t_idx)][0:4] not in years_in:
                ten_dates_aves.append(dates_aves[int(t_idx)])
                years_in.append(dates_aves[int(t_idx)][0:4])
            else:
                ind = dates_aves.index(dates_aves[int(t_idx)])
                del max_indices_aves[ind]
                del dates_aves[ind]
                ten_idx_aves = list(np.array(max_indices_aves).argsort()[-10:][::-1])
        if len(ten_dates_aves) == 10: break
    return ten_dates_sums, ten_dates_aves

########
#M A I N
########
if __name__ == '__main__' :
    years = range(1951,2012)
    #years = range(1951, 1953)
    data_dir = '/media/DataSets/loca/'
    for var_name in ['tmin', 'tmax']:
        print 'VAR ' + var_name
        cold_dates_sums, cold_dates_aves = get_cold_days_from_index_all_domain(var_name, years, data_dir)
        #cold_dates = get_cold_days_from_index(var_name, years, data_dir)
        #cold_dates = get_cold_days_from_pca_1(var_name, data_dir)
        print('SUMS: ')
        print cold_dates_sums
        print('AVES: ')
        print cold_dates_aves
