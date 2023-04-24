from urllib.request import Request, urlopen
import json
import datetime

targetFile = '../monthly_data_series/data_series_nov.json'

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

	req = Request('https://blue.demo.data-humdata-org.ahconu.org/api/action/hdx_dataseries_link')

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
		if index>17005 and series['type']=='data series':
			print('Updating series')
			print(datetime.datetime.now().time())
			updateDataset(dataset['id'],series['series'])
		index = index+1

