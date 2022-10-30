import json

packageFile = 'hdxMetaDataScrape.json'

print('Loading file')
with open(packageFile) as json_file:
	packages = json.load(json_file)

output = {};

#group data series based on same tags from the same organisation
for package in packages:
	output[package['id']] = package['title']

with open('package_title_lookup.json', 'w', encoding='utf-8') as f:
	json.dump(output, f, ensure_ascii=False, indent=4)