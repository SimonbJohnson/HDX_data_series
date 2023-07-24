from urllib.request import Request, urlopen
import json
import datetime

targetFile = '../monthly_data_series/data_series_apr_wfp_update.json'

with open(targetFile) as json_file:
	dataseries = json.load(json_file)

with open('auth.json') as json_file:
	authVar =  json.load(json_file)

print(authVar['authtoken'])


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

index = 0
for series in dataseries:
	for dataset in series['datasets']:
		print(index)
		if index>-1 and series['type']=='data series':
			print('Updating series')
			print(dataset['name'])
			print(datetime.datetime.now().time())
			#updateDataset(dataset['id'],series['series'])
			try:
				updateDataset(dataset['id'],series['series'])
			except:
				print('error')
		index = index+1

