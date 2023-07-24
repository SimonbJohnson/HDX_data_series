import json
import datetime
import ckanapi, json
import math
from urllib.request import Request, urlopen


def updateDataset(datasetid,dataseries):
	d = {"dataseries_name": dataseries, "id": datasetid}
	print(d)
	data = json.dumps(d)
	data = data.encode()

	req = Request('https://data.humdata.org/api/action/hdx_dataseries_link')

	req.add_header('Content-Type', 'application/json')
	req.add_header('Authorization', authVar['authtoken'])

	response_dict = json.loads(urlopen(req, data).read())
	
	if(response_dict['success']==True):
		print('success')
	else:
		print('Fail')

	print(datetime.datetime.now().time())

def removeDataset(datasetid,dataseries):
	d = {"dataseries_name": dataseries, "id": datasetid}
	print(d)
	data = json.dumps(d)
	data = data.encode()

	req = Request('https://data.humdata.org/api/action/hdx_dataseries_unlink')

	req.add_header('Content-Type', 'application/json')
	req.add_header('Authorization', authVar['authtoken'])

	response_dict = json.loads(urlopen(req, data).read())
	
	if(response_dict['success']==True):
		print('success')
	else:
		print('Fail')

	print(datetime.datetime.now().time())


def downloadCurrentState():

	CKAN_URL = "https://data.humdata.org/"
	"""Base URL for the CKAN instance."""

	def find_datasets(start, rows):
	    """Return a page of HXL datasets."""
	    return ckan.action.package_search(start=start, rows=rows)

	# Open a connection to HDX
	ckan = ckanapi.RemoteCKAN(CKAN_URL)

	result = find_datasets(0, 0)
	result_total_count = result["count"]
	numOfFiles =  result["count"]
	#loops = int(math.ceil(numOfFiles/1000))
	output = []
	loops = 100
	j=0
	for i in range(0, loops):
	    print(i)
	    result = find_datasets(1000*i, 1000)
	    packages = result["results"]
	    print(packages)
	    output  = output + packages
	with open('../working files/hdxMetaDataScrape_dataseries.json', 'w') as file:
	    json.dump(output, file)

def createLookUpFile(packages):

	output2 = {}
	for package in packages:
		if 'dataseries_name' in package:
			output2[package['id']] = package['dataseries_name']

	return output2


targetFile = '../monthly_data_series/data_series_jul.json'

with open(targetFile) as json_file:
	dataseries = json.load(json_file)

with open('auth.json') as json_file:
	authVar =  json.load(json_file)


print(authVar['authtoken'])

downloadCurrentState()

with open('../working files/hdxMetaDataScrape_dataseries.json', 'r') as file:
	packages = json.load(file)

lookUp = createLookUpFile(packages)

index = 0


## data series to be added/changed
for series in dataseries:
	for dataset in series['datasets']:
		print(index)
		if index>0 and series['type']=='data series':
			if dataset['id'] in lookUp: 
				oldSeries = lookUp[dataset['id']]
				if oldSeries != series['series']:
					print('Updating series')
					print(datetime.datetime.now().time())
					updateDataset(dataset['id'],series['series'])
			else:
				print('Updating series')
				print(datetime.datetime.now().time())
				try:
					updateDataset(dataset['id'],series['series'])
				except:
					print('404 in updating')
			
		index = index+1

#data series to be removed

for series in dataseries:
	for dataset in series['datasets']:
		if dataset['id'] in lookUp:
			del lookUp[dataset['id']]

for dataset in lookUp:
	removeDataset(dataset,None)

