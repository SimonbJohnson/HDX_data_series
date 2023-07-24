import json
import datetime
import ckanapi, json
import math
import csv
from urllib.request import Request, urlopen

targetFile = '../monthly_data_series/data_series_jul.json'

url = 'https://data.humdata.org/dataset?dataseries_name='

with open(targetFile) as json_file:
	dataseries = json.load(json_file)

output = [['ID','Data series name','Link']]
for series in dataseries:
	if series['type'] == 'data series':
		name = series['series']
		link = url + series['series']
		row = [series['id'],series['series'],link]
		output.append(row)


with open("../dataseries_summary_jun.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(output)