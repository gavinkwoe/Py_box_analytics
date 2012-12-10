import pandas as pd
import numpy as np
import datetime
import MySQLdb

# parameters setup
max_ping_interval    = 120      		# max interval allowed between pings in the same visit, in seconds
sig_str_threshold    = -60     			# signal strength above which walk-in is likely
dwell_time_threshold = 60      			# dwell time above which walk-in is likely, in seconds
walk_in_rule		 = 'walk_in_r2'		# select rule walk_in_rX, x in [1,2,3], to filter out walk_in from those who didn't
	

def data_cleaning(results):
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
 
	global clean
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

	# filter out devices that are not manufactured by a major phone manufacturer
	prefix_map = pd.read_table('prefix_mapping.txt', delimiter='\t', names=['Prefix','Manuf'], index_col='Prefix')
	apply_filter = lambda x: prefix_map.xs(x[0:8].upper()) if x[0:8].upper() in prefix_map.index else 'Not Phone'
	clean['manuf'] = clean['phone_id'].apply(apply_filter)
	clean = clean[clean['manuf'] != 'Not Phone']

	# dropping the element containing a list of signal strengths
	clean = clean.drop('sig_str', axis=1).reindex(columns=['phone_id','start_time', 'end_time','dwell_time','max_str','avg_str', \
													       'manuf','walk_in_r1','walk_in_r2','walk_in_r3','t_bracket'])
	# print clean[:100] #clean after all cleaning and format change

	

	