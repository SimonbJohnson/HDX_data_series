import csv
import json

lastFile = '../monthly_data_series/data_series_oct.json'
targetFile = '../monthly_data_series/data_series_nov.json'
packageLookupFile = '../working files/package_title_lookup_nov.json'

changeFiles = ['nov/november_cods_approved.csv','nov/november_matchedtoone_approved.csv','nov/november_matchedtomany_approved.csv']
newFile = 'nov/november_new_approved.csv'

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
	with open('../monthly_data_series/input_files/'+file, 'r') as csvfile:
		reader = csv.reader(csvfile)
		next(reader)
		for row in reader:
			if row[0]=='Approved' or row[0]=='Exclude':
				dataseriesID = row[6]
				if row[0]=='Exclude':
					dataseriesID = 0
				print(row[6])
				datasets = row[7].split('|')
				dataseriesIndex = getDataseriesIndex(int(dataseriesID))
				for dataset in datasets:
					datasetName = packageLookup[dataset]
					dataseries[dataseriesIndex]['datasets'].append({'id':dataset,'key':datasetName})

with open('../monthly_data_series/input_files/'+newFile, 'r') as csvfile:
	reader = csv.reader(csvfile)
	next(reader)
	currentID = highestDataseriesID(dataseries)
	for row in reader:
		if row[0]=='Create':
			currentID = currentID + 1
			series = {'id':currentID,'series':row[1],'datasets':[],'count':0,'type':'data series'}
			datasets = row[7].split('|')
			for dataset in datasets:
				datasetName = packageLookup[dataset]
				series['datasets'].append({'id':dataset,'key':datasetName})
			series['count'] = len(datasets)
			dataseries.append(series) 



with open(targetFile, 'w', encoding='utf-8') as f:
	json.dump(dataseries, f, ensure_ascii=False, indent=4)

