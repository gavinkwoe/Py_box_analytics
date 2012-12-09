#!/usr/bin/python

import pandas as pd
import numpy as np
import datetime
import MySQLdb

# parameters setup
max_ping_interval    = 120      		# max interval allowed between pings in the same visit, in seconds
sig_str_threshold    = -60     			# signal strength above which walk-in is likely
dwell_time_threshold = 60      			# dwell time above which walk-in is likely, in seconds
walk_in_rule		 = 'walk_in_r2'		# select rule walk_in_rX, x in [1,2,3], to filter out walk_in from those who didn't

# read raw data from MySQL table: box_upload
con 	 = MySQLdb.connect("localhost","guest","guest123","TEST" )
cur 	 = con.cursor()
read_raw = "SELECT * FROM box_upload LIMIT 0, 10"

try:
   cur.execute(read_raw)
   results = cur.fetchall()

except:
   print 'Error: Something went wrong while importing data from MySQL DB'

raw              = pd.DataFrame(list(results), columns=['SQL_ID','Ping_Date', 'Ping_Time', 'Strength', \
														'BSSID', 'Destination(DA)', 'Source(SA)', 'Type'])
parse_datetime   = lambda x: pd.datetools.dateutil.parser.parse(x, yearfirst=True)
raw['Date_Time'] = raw['Ping_Date'].map(str) + ' ' + raw['Ping_Time']
raw['Date_Time'] = raw['Date_Time'].apply(parse_datetime)
raw 			 = raw.drop(['SQL_ID','Ping_Date','Ping_Time','BSSID','Destination(DA)'], axis=1)
raw 			 = raw.reindex(columns=['Date_Time','Strength','Source(SA)','Type'])
# print raw   # rawdata from mysql after quick cleaning

prob_request = raw[raw['Type'].isin(['PREQ'])].reset_index(drop=True, inplace=True)
# print prob_request # filtered out prob_requests

clean = pd.DataFrame()

# loop through data by each unqiue phone_id in given data
for phone_id in prob_request['Source(SA)'].unique():
	
	# perform analyses for an invidual phone_id
	instance = prob_request[prob_request['Source(SA)'] == phone_id].reset_index(drop=True, inplace=True)
	# print instance # data frame with data of one phone_id)
	visits   = [{'start_time':pd.datetime(2012,1,1), 'end_time':pd.datetime(2012,1,1)}]
	
	for row_index, row in instance.iterrows():
		
		dt = (row['Date_Time'] - visits[-1]['end_time']).total_seconds()

		if dt < max_ping_interval:
			visits[-1]['end_time']   = row['Date_Time']
			visits[-1]['dwell_time'] = visits[-1]['dwell_time'] + dt
			visits[-1]['sig_str'].append(row['Strength']) # record signal strengths of pings
			visits[-1]['max_str']    = max(visits[-1]['max_str'], row['Strength']) 
			
	 	else:
			visits.append({'start_time':row['Date_Time'], 'end_time':row['Date_Time'],'dwell_time':0, \
				           'sig_str':[row['Strength']], 'max_str': row['Strength']})
		# print visits # result of visits after a full loop on one phone_id

	visits_df = pd.DataFrame(visits)[1:] # intrim data frame to prevent overwriting clean
	visits_df['phone_id'] = phone_id
	clean = clean.append(visits_df)
	# print clean #clean after appending new data frame

# print clean #clean after looping through all data in question

# add mean signal strengh as a new column
f_mean = lambda x: np.array(x).mean()
clean['avg_str'] = clean['sig_str'].apply(f_mean)

# apply rules to differeciate walk_in and pass_by, add flags to new columns
rule_1 = lambda x: True if x['dwell_time'] > dwell_time_threshold else False
rule_2 = lambda x: True if x['max_str'] > sig_str_threshold else False
rule_3 = lambda x: True if ((x['max_str'] > sig_str_threshold) or (x['dwell_time'] > dwell_time_threshold)) else False
clean['walk_in_r1'] = clean.apply(rule_1, axis=1)  
clean['walk_in_r2'] = clean.apply(rule_2, axis=1)  
clean['walk_in_r3'] = clean.apply(rule_3, axis=1)  

# add time bracket column, to be used for later aggregation
find_bracket = lambda x: x - datetime.timedelta(minutes=x.minute % 3, seconds=x.second, microseconds=x.microsecond)
clean['t_bracket'] = clean['start_time'].apply(find_bracket)
# print clean[:99] # clean before dropping columns and reindexing

clean = clean.drop('sig_str', axis=1).reindex(columns=['phone_id','start_time', 'end_time','dwell_time',\
													   'max_str','avg_str','walk_in_r1','walk_in_r2','walk_in_r3','t_bracket'])
# print clean #clean after all cleaning and format change

# write clean into MySQL table: clean
write_clean = """INSERT INTO clean(phone_id,start_time,end_time,dwell_time, \
				           max_str,avg_str,walk_in_r1,walk_in_r2,walk_in_r3,t_bracket) \
		 		 		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

for row_index, row in clean.iterrows():	
	try:
		cur.execute(write_clean,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
		con.commit()
	except:
		print 'Error while writing to MySQL table: Clean'
		
# # delete rows just been processed from MySQL table: box_upload
# reduce_raw = """DELETE FROM box_upload LIMIT 10"""

# try: 
# 	cur.execute(reduce_raw)
# 	con.commit()
# except:
# 	con.rollback()
# 	print 'Error while reducing MySQL table: box_upload'

con.close()

# preparing data into plottable formant
# walk_in: people came in store; pass_by: people pass by but didn't come in; walk_in + pass_by = total visits
output   		         = pd.DataFrame(index=clean['t_bracket'].unique()).sort_index()
output['walk_in']        = clean.groupby('t_bracket')[walk_in_rule].sum()
output['pass_by'] 		 = clean[clean[walk_in_rule]==False].groupby('t_bracket')['phone_id'].count() 
output['avg_dwell_time'] = clean[clean[walk_in_rule]==True].groupby('t_bracket')['dwell_time'].mean()
print output

# find out unique visitors during the period of the data given
unique_walk_in = len(clean[clean[walk_in_rule]==True]['phone_id'].unique())
unique_pass_by = len(clean[clean[walk_in_rule]==False]['phone_id'].unique())

print 'unique walk-in during the given period:', unique_walk_in
print 'unique pass-by during the given period:', unique_pass_by
