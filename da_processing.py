import pandas as pd
import numpy as np
import datetime
import MySQLdb

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
