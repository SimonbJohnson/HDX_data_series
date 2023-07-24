import csv
import json

def getDataSeriesIndex(dataseries,name):
	index = 0
	found = -1
	for series in dataseries:
		if series['series']==name:
			found = index
		index = index+1
	return found

def prepLookUp(oldLookup):
	newLookup = {}
	for key in oldLookup:
		value = oldLookup[key]
		newLookup[value] = key

	return newLookup

dataseriesFile = '../monthly_data_series/data_series_apr_pre_additions.json'
dataseries = []
with open(dataseriesFile) as json_file:
	dataseries = json.load(json_file)

lookupFile = '../working files/package_title_lookup_apr.json'
with open(lookupFile) as json_file:
	lookup = json.load(json_file)

lookup = prepLookUp(lookup)

with open('old/datasets_to_add_apr.csv', 'r') as csvfile:
	reader = csv.reader(csvfile)
	next(reader)
	for row in reader:
		print(row)
		print(row[0])
		index = getDataSeriesIndex(dataseries,row[0])
		if index>-1:
			dataSetID = lookup[row[1]]
			print(dataSetID)
			dataseries[index]['datasets'].append({"id": dataSetID,"name": row[1]})
		else:
			print('Data series not found')

print(dataseries)
with open('../monthly_data_series/data_series_apr.json', 'w', encoding='utf-8') as f:
    json.dump(dataseries, f, ensure_ascii=False, indent=4)

