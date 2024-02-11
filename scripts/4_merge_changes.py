import csv
import json

#change these
lastFile = '../monthly_data_series/23-12-data_series.json'
targetFile = '../monthly_data_series/24-01-data_series.json'
packageLookupFile = '../process_files/package_title_lookup/24-01-package_title_lookup.json'

changeFiles = ['24-01/24-01-checks - matchedToMany.csv','24-01/24-01-checks - matchedToOne.csv']
newFiles = ['24-01/24-01-checks - new_1.csv','24-01/24-01-checks - new_3.csv','24-01/24-01-checks - new_4.csv','24-01/24-01-checks - new_5.csv','24-01/24-01-checks - new_7.csv','24-01/24-01-checks - new_8.csv']

def getDataseriesIndex(seriesID):
	index = -1
	count = 0
	for series in dataseries:
		if seriesID == series['id']:
			index = count
		count=count+1
	return index

def transformDataseriesToNewForm(dataseries):
	output = []
	for key in dataseries:
		for series in dataseries[key]:
			series['type'] = key
			output.append(series)
	return output

def highestDataseriesID(dataseries):
	maxID = 0
	for series in dataseries:
		if int(series['id']) > maxID:
			maxID = int(series['id'])
	return maxID

with open(lastFile) as json_file:
	dataseries = json.load(json_file)

with open(packageLookupFile) as json_file:
	packageLookup = json.load(json_file)

for file in changeFiles:
	print(file)
	with open('../process_files/csv_outputs/'+file, 'r') as csvfile:
		reader = csv.reader(csvfile)
		next(reader)
		for row in reader:
			if row[0]=='Approved' or row[0]=='Exclude':
				dataseriesID = row[6]
				if dataseriesID[0:5] == 'none|':
					dataseriesID = dataseriesID[5:]
				if row[0]=='Exclude':
					dataseriesID = 0
				print(dataseriesID)
				datasets = row[7].split('|')
				dataseriesIndex = getDataseriesIndex(int(dataseriesID))
				for dataset in datasets:
					datasetName = packageLookup[dataset]
					dataseries[dataseriesIndex]['datasets'].append({'id':dataset,'key':datasetName})

for file in newFiles:
	with open('../process_files/csv_outputs/'+file, 'r') as csvfile:
		reader = csv.reader(csvfile)
		currentID = highestDataseriesID(dataseries)
		currentID = currentID + 1
		index = 0
		series = {'id':currentID,'series':'','datasets':[],'count':0,'type':'data series'}
		exclude = False
		dataExcludeIndex = getDataseriesIndex(0)
		for row in reader:
			if index == 0:
				if row[0]=='clean' or row[0]=='Clean':
					series['type']='clean'
				if row[0]=='exclude' or row[0]=='Exclude':
					exclude = True
				series['series'] = row[0]
			if index>1:
				if exclude == True:
					dataseries[dataseriesIndex]['datasets'].append({'id':row[0],'key':row[1]})
				else:
					series['datasets'].append({'id':row[0],'key':row[1]})
			index= index+1
		series['count'] = len(datasets)
		dataseries.append(series) 

with open(targetFile, 'w', encoding='utf-8') as f:
	json.dump(dataseries, f, ensure_ascii=False, indent=4)

