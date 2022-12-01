import json 
import csv

output = {'data series':[],'excluded':[],'clean':[]}
names = {'data series':{},'excluded':{},'clean':{}}

with open('working files/package_title_lookup.json') as json_file:
	titleLookUp = json.load(json_file)

total = 0
with open('monthly_data_series/Final data series september.csv', 'r') as csvfile:
	reader = csv.reader(csvfile)
	count = 1
	rowCount = 1
	for row in reader:
		if count>1:
			datasets = []
			datasetIDs = row[12].replace(" & ","|").split("|")
			index = 14
			for ID in datasetIDs:
				try:
					datasets.append({'id':ID,'name':titleLookUp[ID]})
					index = index +1
				except:
					print('data set removed')

			dataSeries = {'id':count,'series':row[1],'row':rowCount,'datasets':datasets,'count':len(datasets)}
			

			#check if data series name has already been used
			#merge if so
			seriesTypeKey = ''
			if row[0]=='Yes':
				seriesTypeKey = 'data series'
			if row[0]=='Clean':
				seriesTypeKey = 'clean'
			if row[0]=='Exclude':
				seriesTypeKey = 'excluded'
			if seriesTypeKey!= '':
				if row[1] in names[seriesTypeKey]:
					index = names[seriesTypeKey][row[1]]
					output[seriesTypeKey][index]['count'] = output[seriesTypeKey][index]['count'] + len(datasets)
					output[seriesTypeKey][index]['datasets'] = output[seriesTypeKey][index]['datasets'] + datasets
				else:
					output[seriesTypeKey].append(dataSeries)
					names[seriesTypeKey][row[1]] = len(output[seriesTypeKey]) - 1
				total = total + len(datasets)
		count=count+1
		rowCount = rowCount + 1

	with open('data_series_sept_with_merges.json', 'w', encoding='utf-8') as f:
		json.dump(output, f, ensure_ascii=False, indent=4)


print(total)