#########################################
### create search index for browsing site
#########################################

# improvements to make
# don't allow country tags to be secondary tag of another country

import json
import os
import csv
import Levenshtein
from difflib import SequenceMatcher
import operator


def substringCounter(names):
	substring_counts={}
	for i in range(0, len(names)):
	    for j in range(i+1,len(names)):
	        string1 = names[i]
	        string2 = names[j]
	        match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
	        matching_substring=string1[match.a:match.a+match.size]
	        if(matching_substring not in substring_counts):
	            substring_counts[matching_substring]=1
	        else:
	            substring_counts[matching_substring]+=1

	return (max(substring_counts.items(), key=operator.itemgetter(1))[0])

def regroupOnName(series):
	output = {}
	for row in series:
		seriesValue = row[1]+' '+row[0]
		if seriesValue in output:
			output[seriesValue][1] = output[seriesValue][1] + ' & ' + row[1]
			output[seriesValue][2] = output[seriesValue][2] + ' & ' + row[2]
			output[seriesValue][3] = output[seriesValue][3] + ' & ' + row[3]
			output[seriesValue][4] = output[seriesValue][4] + '|' + row[4]
			output[seriesValue][5] = output[seriesValue][5] + row[5]
			#needs to combine series names
		else:
			output[seriesValue] = row

	outputseries = []
	for key in output:
		outputseries.append(output[key])
	return outputseries

packageFile = 'working files/hdxMetaDataScrape_oct.json'

print('Loading file')
with open(packageFile) as json_file:
	packages = json.load(json_file)

output = {};


#group data series based on same tags from the same organisation
for package in packages:
	#if package['metadata_created']<'2022-01-01':
	taglist = []
	for tag in package['tags']:
		taglist.append(tag['display_name'])
	taglist = sorted(taglist)
	hashInput = package['organization']['title'] + ''.join(taglist)
	hashValue = hash(hashInput)
	batch = ''
	try:
		batch = package['batch']
	except:
		print('No batch')
	if hashValue in output:
		output[hashValue]['titles'].append(package['title'])
		output[hashValue]['batch'].append(batch)
		output[hashValue]['IDs'].append(package['id'])
	else:
		output[hashValue] = {'titles':[package['title']],'org':package['organization']['title'],'tags':taglist,'batch':[batch],'IDs':[package['id']]}


count = 0
csvOutput = []
for key in output:
	series = output[key]
	#need to add an exception for CODs
	if len(series['titles'])>4 and len(series['tags'])>0:
		print(series)
		count = count+1
		#name = Levenshtein.quickmedian(series['titles'])
		#print(name)
		name = substringCounter(series['titles'])
		print(name)
		csvOutput.append([name,series['org'],'|'.join(series['tags']),'|'.join(series['batch']),'|'.join(series['IDs']),len(series['titles'])] + series['titles'])

#make this work on the JSON structure for easy combintion of name data series
csvOutput = regroupOnName(csvOutput)

with open("series_october.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(csvOutput)

JSONOutput = []
#create JSON output
for key in list(output):
	series = output[key]
	if (len(series['titles'])>4 and len(series['tags'])>0) or 'common operational dataset - cod' in series['tags']:
		JSONOutput.append(series)


#ignore names field as incorrect
with open('data_series_october_first_output.json', 'w', encoding='utf-8') as f:
	json.dump(JSONOutput, f, ensure_ascii=False, indent=4)
