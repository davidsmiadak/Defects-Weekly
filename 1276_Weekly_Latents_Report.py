# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 10:10:39 2023

@author: dsmiadak
"""
import os
import csv
import matplotlib.pyplot as plt
import numpy as np
import math
import PyUber
import warnings
from datetime import datetime, timedelta
import pandas as pd
from urllib.error import HTTPError
warnings.filterwarnings('ignore')
from collections import defaultdict
from itertools import combinations

def Defect_Counter(points):
    
    Angle = 0
    #WaferX, WaferY = points
    ExtremeEdgeCounts_NonFilmPullingRegion = 0
    CenterCounts = 0
    
    for WaferX, WaferY in points:
        WaferR = math.sqrt((WaferX)**2 + (WaferY)**2)
        if WaferX != 0 and WaferY != 0:
                Angle = math.degrees(2*math.atan(WaferY/(WaferX+math.sqrt(WaferX**2+WaferY**2))))
        # This laundry list of angles defines what is a non-fill pulling region
        if WaferR <= 135000000:
            CenterCounts = CenterCounts + 1
        if WaferR >= 145000000:
                if (0 <= Angle <= 55 or 62 <= Angle <= 110 or 130 <= Angle <= 147 or 
                152 <= Angle <= 180 or -180 < Angle <= -152 or -132 <= Angle <= -110 or 
                -102 <= Angle <= -63 or -45 <= Angle <= -30 or -13 <= Angle <= 0):
                    ExtremeEdgeCounts_NonFilmPullingRegion = ExtremeEdgeCounts_NonFilmPullingRegion + 1
    return(CenterCounts, ExtremeEdgeCounts_NonFilmPullingRegion)

file_path = 'C:/Users/DSMIADAK/OneDrive - Intel Corporation/Documents/Defects/_Projects/1276 Auto Pull/SQL_Query_Data.csv'

print("YAS Database Query: Started")
current_time = datetime.now()
formatted_current_time = current_time.strftime("%Y/%m/%d %H:%M:%S")
#print(formatted_current_time)

time_60_days_earlier = current_time - timedelta(days=60)
formatted_time_60_days_earlier = time_60_days_earlier.strftime("%Y/%m/%d %H:%M:%S")
#print(formatted_time_7_hours_earlier)

sql="""Select *
From INSP_WAFER_SUMMARY
  Left Join INSP_DEFECT On INSP_WAFER_SUMMARY.INSPECTION_TIME =
    INSP_DEFECT.INSPECTION_TIME And INSP_WAFER_SUMMARY.WAFER_KEY =
    INSP_DEFECT.WAFER_KEY And INSP_DEFECT.DEFECT_ID < 1001
  Left Join CLASS On INSP_DEFECT.CLASS_NUMBER = CLASS.CLASS_ID
  Left Join INSP_WAFER_IMAGE i On INSP_DEFECT.INSPECTION_TIME =
    i.INSPECTION_TIME And INSP_DEFECT.WAFER_KEY = i.WAFER_KEY And
    INSP_DEFECT.DEFECT_ID = i.DEFECT_ID
  Inner Join (Select INSP_WAFER_SUMMARY.INSPECTION_TIME,
    INSP_WAFER_SUMMARY.LOT_ID,
    Count(Distinct INSP_WAFER_SUMMARY.WAFER_ID) As inspection_wafer_count
  From INSP_WAFER_SUMMARY INSP_WAFER_SUMMARY
  Group By INSP_WAFER_SUMMARY.INSPECTION_TIME,
    INSP_WAFER_SUMMARY.LOT_ID) ws2 On ws2.INSPECTION_TIME =
    INSP_WAFER_SUMMARY.INSPECTION_TIME And
    ws2.LOT_ID = INSP_WAFER_SUMMARY.LOT_ID
Where INSP_WAFER_SUMMARY.LAYER_ID In ( 
'6RESIST4347THK750A_E044_PST') And 
To_Char(INSP_WAFER_SUMMARY.INSPECTION_TIME, 'YYYY/MM/DD hh24:mi:ss') 
Between '""" + formatted_time_60_days_earlier + "' And '" + formatted_current_time + "'"

conn = PyUber.connect('D1D_PROD_YAS_1276')
df = pd.read_sql(sql, conn)

#Remove leading/trailing whitespace
df = df.applymap(lambda x: x.strip() if type(x)==str else x)
df = df.drop_duplicates()
adder_data = df[df['ADDER']==1]
#
# Selecting specific columns from database
# 0=INSPECTION_TIME, 5=LOT_ID, 7=SLOT_ID, 13=PROCESS_EQUIP_id, 10=LAYER_ID, 81=WAFER_X, 82=WAFER_Y, 87=ADDER
#
adder_data = adder_data.iloc[:, [0, 5, 7, 13, 10, 81, 82]]
adder_data = adder_data.drop_duplicates()
#Center x and y
adder_data['WAFER_X'] =  adder_data['WAFER_X'] - 150000000
adder_data['WAFER_Y'] =  adder_data['WAFER_Y'] - 150000000

#print(adder_data)
#adder_data.to_csv(file_path, index=False)    

print("YAS Database Query: Finished")
print("Defect Counters: Started")

pd.options.display.max_rows = 4000

# Define a dictionary to store unique combinations as keys and corresponding WaferX and WaferY values as pairs
combination_data = {}

grouped = adder_data.groupby(['INSPECTION_TIME', 'LOT_ID', 'SLOT_ID', 'PROCESS_EQUIP_ID']).agg(list)

filtered_groups = grouped[grouped.index.get_level_values('PROCESS_EQUIP_ID').isin(['SAA537', 'SAF530', 'SAF531','SAF532','SAF533','SAF534',
                                                                                   'SAF535', 'SAF536', 'SAU438', 'SAU439', 'SAU446',
                                                                                   'SAV454', 'SAV456','SAV462'])]
unique_combinations = filtered_groups.index.tolist()

sorted_pairs = []
line_counts = []

for combination in unique_combinations:
    
    pairs = []
    shifted_pairs = []
    
    # Create x, y tuple
    for i in range(len(grouped.loc[combination]['WAFER_X'])):
        pairs.append((grouped.loc[combination]['WAFER_X'][i], grouped.loc[combination]['WAFER_Y'][i]))
    # Sort by x value   
    pairs.sort(key=lambda x: x[0]) 
    sorted_pairs.append(pairs)
    
print('PROCESS_EQUIP_ID, LOT_ID, SLOT_ID, INSPECTION_TIME, Center Defects')
# Print unique combinations and sorted pairs

columns = ['Process Equipment', 'Lot', 'Slot', 'Inspection Time', 'Center Defects', 'Extreme Edge Defects']
output_df = pd.DataFrame(columns=columns)
row_index = 0

#print(pairs)
for idx, combination in enumerate(unique_combinations):
    Center_Counts, XEdge_Counts = Defect_Counter(sorted_pairs[idx])
    output_df.loc[idx] = [combination[3], combination[1], combination[2], combination[0], Center_Counts, XEdge_Counts]

output_df.to_csv(file_path, index=False)

print("Defect Counters: Completed")

## Need to define tools,