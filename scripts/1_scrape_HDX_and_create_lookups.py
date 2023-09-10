import ckanapi, json
import math
import datetime
from datetime import datetime

#file prefix

month = datetime.now().month
year = datetime.now().year

monthPrefix = 'test_'+str(year)[2:4]+'-'+str(month).zfill(2)+'-'


def reduceMetaData(packages):
    output = []
    for package in packages:
        data = {}
        data['tags'] = []
        for tag in package['tags']:
            data['tags'].append(tag['display_name'])
        if 'cod_level' in package:
            data['cod_level'] = package['cod_level']
        if 'batch' in package:
            data['batch'] = package['batch']
        data['organization'] = {}
        data['organization']['title'] =  package['organization']['title']
        data['id'] = package['id']
        data['title'] = package['title']
        if 'dataseries_name' in package:
            data['dataseries_name'] = package['dataseries_name']
        output.append(data)
    return output


CKAN_URL = "https://data.humdata.org"
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
    output  = output + packages

output = reduceMetaData(output)
with open('../process_files/HDXMetaDataScrape/'+monthPrefix+'hdxMetaDataScrape.json', 'w') as file:
    json.dump(output, file)

output2 = {}
for package in output:
    output2[package['id']] = package['title']

with open('../process_files/package_title_lookup/'+monthPrefix+'package_title_lookup.json', 'w', encoding='utf-8') as f:
    json.dump(output2, f, ensure_ascii=False, indent=4)