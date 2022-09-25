import json 
import csv

output = []

with open('data/Data series September 2022 - series_september.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    count = 1
    rowCount = 1
    for row in reader:
    	if row[2] == 'Yes':
    		datasets = []
    		datasetIDs = row[11].split("|")
    		index = 12
    		for ID in datasetIDs:
    			datasets.append({'id':ID,'name':row[index]})
    			index = index +1
    		output.append({'id':count,'series':row[1],'row':rowCount,'datasets':datasets})
    		count=count+1
    	rowCount = rowCount + 1

with open('data_series_sept.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=4)