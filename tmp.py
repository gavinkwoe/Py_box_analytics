#!/usr/bin/python

import pandas as pd
import numpy as np
import datetime
import MySQLdb

# con = MySQLdb.connect("localhost","guest","guest123","TEST" )
# cur = con.cursor()

# read_filter = """SELECT * FROM phone_filter"""

# cur.execute(read_filter)
# filter_map  = cur.fetchall()

# prefix_map = pd.read_table('prefix_mapping.txt', delimiter='\t', names=['Prefix','Manuf'], index_col='Prefix')
# print prefix_map[:100]
# # print prefix_map['Prefix']#.apply(lambda x: x.lower())
# # print prefix_map[:100]

# x = '00:16:44:69:ab:7a'
# x = '00:26:bb:0e:93:27'

# try:
# 	a = str(prefix_map.xs(x[0:8].upper()))
# except: 
# 	a = 'not phone'
# # else:
# # 	a = prefix_map.xs(x[0:8].upper()) 
# print a, type(a)

# prefix_map = pd.read_table('prefix_mapping.txt', delimiter='\t', names=['Prefix','Manuf'], index_col='Prefix')
# 	# apply_filter = lambda x: try: prefix_map.xs(x['phone_id'][0:8].upper()) except: 'Not phone'
# 	# clean['manuf'] = clean.apply(apply_filter, axis=1)
# manuf = pd.Series()
# print manuf
# for row_index, row in prefix_map.iterrows():
# 	try: 
# 		print str(row['phone_id'][0:8].upper())
# 		manuf = manuf.append(str(row['phone_id'][0:8].upper()))
# # 		print manuf
# # 	except:
# # 		# manuf = manuf.append('Not Phone')
# # 		pass
# # 		print manuf

# # print manuf

# prefix_map = pd.read_table('prefix_mapping.txt', delimiter='\t', names=['Prefix','Manuf'], index_col='Prefix')
# 	# apply_filter = lambda x: try: prefix_map.xs(x['phone_id'][0:8].upper()) except: 'Not phone'
# 	# clean['manuf'] = clean.apply(apply_filter, axis=1)

# print prefix_map.index

# if x[0:8].upper() in prefix_map.index:
# 	print 'yes'
# else:
# 	print 'no'


# read raw data from MySQL table: box_upload
con 	 = MySQLdb.connect("localhost","guest","guest123","TEST" )
cur 	 = con.cursor()
read_raw = "SELECT * FROM box_upload LIMIT 0, 200"

cur.execute(read_raw)
results = cur.fetchall()

raw              = pd.DataFrame(list(results), columns=['SQL_ID','Ping_Date', 'Ping_Time', 'Strength', \
														'BSSID', 'Destination(DA)', 'Source(SA)', 'Type'])
parse_datetime   = lambda x: pd.datetools.dateutil.parser.parse(x, yearfirst=True)
raw['Date_Time'] = raw['Ping_Date'].map(str) + ' ' + raw['Ping_Time']
raw['Date_Time'] = raw['Date_Time'].apply(parse_datetime)
raw 			 = raw.drop(['SQL_ID','Ping_Date','Ping_Time','BSSID','Destination(DA)'], axis=1)
raw 			 = raw.reindex(columns=['Date_Time','Strength','Source(SA)','Type'])


preq = raw[raw['Type']=='PREQ'].reset_index(drop=True, inplace=True)
pres = raw[raw['Type']=='PRES'].reset_index(drop=True, inplace=True)

# print preq[:99]
# print pres[:99]

count = 0

for row_index, row in preq.iterrows():
	# print row['Source(SA)']
	# print '='*20
	if row['Source(SA)'] in pres['Source(SA)'].values:
		count += 1
		print row['Source(SA)']

print count
