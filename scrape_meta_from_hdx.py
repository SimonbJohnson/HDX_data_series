import ckanapi, json
import math

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
loops = 20
j=0
for i in range(0, loops):
    print i
    result = find_datasets(1000*i, 1000)
    packages = result["results"]
    print packages
    output  = output + packages
with open('hdxMetaDataScrape.json', 'w') as file:
    json.dump(output, file)