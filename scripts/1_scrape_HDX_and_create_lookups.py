import ckanapi, json
import math

#month suffix
#update this variable
monthSuffix = 'mar'



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
    print(packages)
    output  = output + packages
with open('../working files/hdxMetaDataScrape_'+monthSuffix+'.json', 'w') as file:
    json.dump(output, file)


output2 = {}
for package in output:
    output2[package['id']] = package['title']

with open('../working files/package_title_lookup_'+monthSuffix+'.json', 'w', encoding='utf-8') as f:
    json.dump(output2, f, ensure_ascii=False, indent=4)