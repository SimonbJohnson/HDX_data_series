import json
import csv

lastMonthFile = 'monthly_data_series/data_series_sept.json'

with open(lastMonthFile) as json_file:
	lastMonth = json.load(json_file)


summary = {'datasets':0,'dataseries':0}

for series in lastMonth['data series']:
	summary['datasets'] = summary['datasets']+series['count']

summary['dataseries'] = len(lastMonth['data series'])

print(summary)

packageFile = 'working files/hdxMetaDataScrape_oct.json'

print('Loading file')
with open(packageFile) as json_file:
	packages = json.load(json_file)

output = {};

totalDownloads = 0
dataseriesDownloads = 0

for package in packages:
	output[package['id']] = package['total_res_downloads']
	totalDownloads = totalDownloads + package['total_res_downloads']

for series in lastMonth['data series']:
	for dataset in series['datasets']:
		if dataset['id'] in output:
			dataseriesDownloads = dataseriesDownloads + output[dataset['id']]

print(dataseriesDownloads)
print(totalDownloads)