#!/usr/bin/python

import pandas as pd
import numpy as np
import datetime
import MySQLdb
import da_clean

# connect to database
con = MySQLdb.connect("localhost","guest","guest123","TEST" )
cur = con.cursor()

# prepare R/W SQL commands
# read_raw    = """SELECT * FROM box_upload WHERE Flag = 0 LIMIT 0, 1000;""" 
read_raw    = """SELECT * FROM box_upload LIMIT 0, 20000""" 
write_clean = """INSERT INTO clean(phone_id,start_time,end_time,dwell_time, \
				           max_str,avg_str,manuf,walk_in_r1,walk_in_r2,walk_in_r3,t_bracket) \
		 		 		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""  
# update_flag = """UPDATE box_upload SET Flag = 1; """
reduce_raw  = """DELETE FROM box_upload LIMIT 20000"""

# read_filter = """SELECT * FROM phone_filter"""

# read in raw data from MySQL
cur.execute(read_raw)
results = cur.fetchall()

# perform data cleaning
da_clean.data_cleaning(results)

try:
	# write results into MySQL table: clean
	for row_index, row in da_clean.clean.iterrows():	
		cur.execute(write_clean,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]))

	# delet rows from MySQL table
	# cur.execute(reduce_raw)
	
	con.commit()

except:
	con.rollback()


con.close()