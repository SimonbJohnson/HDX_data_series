import json
import csv

monthSuffix = 'mar'

def createDataSetLookUp(dataseries):
	datasetLookUp = {}
	for series in dataseries:
		seriesID = series['id']
		for dataset in series['datasets']:
			datasetLookUp[dataset['id']] = {'id':seriesID,'name':series['series'],'type':series['type'],'count':series['count']}
	return datasetLookUp

def listOfKeys(matches):
	output = []
	for key in matches:
		if key!='None':
			output.append(str(key))
	return output

def propertyToList(matches,key):
	output = []
	for match in matches:
		if match!='none':
			output.append(str(match[key]))
	return output

def listOfPropertiesToList(dataseries,key):
	output = []
	for series in dataseries:
		output.append(str(series[key]))
	return output


def candidateSeriesCSV(dataseriess):
	output = [['Action','Notes','title','key','Org','count','Matches','Unmatched','Names']]
	
	for dataseries in dataseriess:
		unmatchedNames = []
		for dataset in dataseries['unmatched']:
			unmatchedNames.append(titleLookUp[dataset])
		seriesTitles = '|'.join(listOfPropertiesToList(dataseries['details'],'name'))
		seriesTypes = '|'.join(listOfPropertiesToList(dataseries['details'],'type'))
		counts = '|'.join(listOfPropertiesToList(dataseries['details'],'count'))
		matches = '|'.join(listOfKeys(dataseries['matches']))
		unmatched = '|'.join(dataseries['unmatched'])
		line = ['','',seriesTitles,seriesTypes,dataseries['org'],counts,matches,unmatched]+unmatchedNames
		output.append(line)
	return output

lastMonthFile = '../monthly_data_series/data_series_feb.json'
thisMonthFile = '../working files/data_series_first_cluster_mar.json'
lookUpFile = '../working files/package_title_lookup_mar.json'

with open(lastMonthFile) as json_file:
	lastMonth = json.load(json_file)

with open(thisMonthFile) as json_file:
	thisMonth = json.load(json_file)

with open(lookUpFile) as json_file:
	titleLookUp = json.load(json_file)

datasetLookUp = createDataSetLookUp(lastMonth)
unmatched = 0

for candidateSeries in thisMonth:
	candidateSeries['matches'] = {'none':0}
	candidateSeries['unmatched'] = []
	candidateSeries['details'] = []
	for dataset in candidateSeries['IDs']:
		if dataset in datasetLookUp:
			dataSeriesID = datasetLookUp[dataset]['id']
			dataSeriesDetails = datasetLookUp[dataset]
			if dataSeriesID in candidateSeries['matches']:
				candidateSeries['matches'][dataSeriesID] = candidateSeries['matches'][dataSeriesID]+1
			else:
				candidateSeries['matches'][dataSeriesID] = 1
				candidateSeries['details'].append(dataSeriesDetails)
		else:
			candidateSeries['matches']['none'] = candidateSeries['matches']['none'] + 1
			unmatched = unmatched + 1
			candidateSeries['unmatched'].append(dataset)
		
summary = {'total':0,'matchedToOne':0,'matchedToMany':0,'new':0,'cods':0}
assessmentSummary = {'total':0,'matchedToOne':0,'matchedToMany':0,'new':0,'cods':0}

output = {'matchedToOne':[],'matchedToMany':[],'new':[],'cods':[]}

for candidateSeries in thisMonth:
	
	summary['total'] = summary['total']+1
	percentNotMatched = candidateSeries['matches']['none'] / len(candidateSeries['IDs'])
	if percentNotMatched < 0.75:
		if len(candidateSeries['matches'])==2:
			if percentNotMatched!=0:
				summary['matchedToOne'] = summary['matchedToOne'] + 1
				assessmentSummary['matchedToOne'] = assessmentSummary['matchedToOne'] + candidateSeries['matches']['none']
				output['matchedToOne'].append(candidateSeries)
		else:
			if percentNotMatched!=0:
				summary['matchedToMany'] = summary['matchedToMany'] + 1
				assessmentSummary['matchedToMany'] = assessmentSummary['matchedToMany'] + candidateSeries['matches']['none']
				output['matchedToMany'].append(candidateSeries)
	else:
		if 'common operational dataset - cod' in candidateSeries['tags']:
			summary['cods'] = summary['cods'] + 1
			assessmentSummary['cods'] = assessmentSummary['cods'] + candidateSeries['matches']['none']
			output['cods'].append(candidateSeries)
		else:
			summary['new'] = summary['new'] + 1
			assessmentSummary['new'] = assessmentSummary['new'] + candidateSeries['matches']['none']
			output['new'].append(candidateSeries)


print('creating spreadsheets')

for key in output:
	if key!='new':
		rows = []
		rows = candidateSeriesCSV(output[key])

		with open("../monthly_data_series/input_files/"+monthSuffix+"/"+key+"_"+monthSuffix+".csv", "w") as f:
		    writer = csv.writer(f)
		    writer.writerows(rows)
	else:
		index = 0
		for candidateSeries in output[key]:
			title = '|'.join(listOfPropertiesToList(candidateSeries['details'],'name'))
			candidateOutput = [[candidateSeries['org']]]
			for dataset in candidateSeries['unmatched']:
				name = titleLookUp[dataset]
				candidateOutput.append([dataset,name])
			with open("../monthly_data_series/input_files/"+monthSuffix+"/"+key+"_"+monthSuffix+"_"+str(index)+".csv", "w") as f:
			    writer = csv.writer(f)
			    writer.writerows(candidateOutput)
			index= index+1


print('Data series count')
print(summary)
print('unmatched')
print(unmatched)
print('assessment summary')
print(assessmentSummary)

