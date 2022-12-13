from urllib.request import Request, urlopen
import json

with open('auth.json') as json_file:
	authVar =  json.load(json_file)


print(authVar['authtoken'])

d = {"dataseries_name": "3W Data Series test", "id": 'south-sudan-operational-presence'}
data = json.dumps(d)   # <-- Dump the dictionary as JSON
data = data.encode()

req = Request('https://blue.demo.data-humdata-org.ahconu.org/api/action/hdx_dataseries_link')

req.add_header('Content-Type', 'application/json')
req.add_header('Authorization', authVar['authtoken'])

response_dict = json.loads(urlopen(req, data).read())

print(response_dict)

