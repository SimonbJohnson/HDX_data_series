import csv
import json

lastFile = '../monthly_data_series/data_series_feb.json'
targetFile = '../monthly_data_series/data_series_mar.json'
packageLookupFile = '../working files/package_title_lookup_mar.json'

changeFiles = ['mar/March Data Series - cods.csv','mar/March Data Series - matchedtomany.csv','mar/March Data Series - matchedtoone.csv']
newFiles = ['mar/March Data Series - new0.csv','mar/March Data Series - new2.csv','mar/March Data Series - new3.csv','mar/March Data Series - new4.csv','mar/March Data Series - new5.csv','mar/March Data Series - new6.csv','mar/March Data Series - new7.csv','mar/March Data Series - new8.csv','mar/March Data Series - new10.csv','mar/March Data Series - new11.csv','mar/March Data Series - new12.csv','mar/March Data Series - new13.csv']
clean = []

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
	with open('../monthly_data_series/input_files/'+file, 'r') as csvfile:
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

