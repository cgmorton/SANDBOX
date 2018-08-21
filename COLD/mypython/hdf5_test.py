import h5py
import numpy as np


f = h5py.File("/media/DataSets/loca/CMCC-CMS/rcp45/2002.h5","r")

print dir(f.attrs)

#Prints variable keys
#print f.keys()
'''
for v in f.keys():
    print f[v]
'''

#print f['pr']
print type(f['tasmin'])
var = np.array(f['tasmin'])

#print np.where(var[1][:][200] > -9998.0)
#print np.where(var[4][58][:] > 0.0)
#print var[1][:][200]
'''
np.set_printoptions(precision=5)
#print (var[0][7][392:461])

sub = var[50][:][:]
sub = sub[sub != 32767]
print sub[sub >-9998.0]
'''
'''
non_fill = np.where(var[1][:][200:205] >-9998.0 )
print  np.where(non_fill < 32767)

print var[1][22][200:205]
'''
'''
<HDF5 dataset "tasmax": shape (365, 585, 1386), type "<i2">
'''

#print dim(f['tasmax'][0])

'''
for key, val in f['tasmax'].attrs.iteritems():
    print key + ":", val
'''

#Print dim(time), dim(lat), dim(lon)
#print len(f['rhsmax']), len(f['rhsmax'][0]), len(f['rhsmax'][0][0])

#print f['rhsmax'][0][0]

'''
for day_data in f['pr']:
    print day_data
'''

f.close()
