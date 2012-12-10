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
x = '00:26:bb:0e:93:27'

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
# 		print manuf
# 	except:
# 		# manuf = manuf.append('Not Phone')
# 		pass
# 		print manuf

# print manuf

prefix_map = pd.read_table('prefix_mapping.txt', delimiter='\t', names=['Prefix','Manuf'], index_col='Prefix')
	# apply_filter = lambda x: try: prefix_map.xs(x['phone_id'][0:8].upper()) except: 'Not phone'
	# clean['manuf'] = clean.apply(apply_filter, axis=1)

print prefix_map.index

if x[0:8].upper() in prefix_map.index:
	print 'yes'
else:
	print 'no'
