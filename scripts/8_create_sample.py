#########################################
### create search index for browsing site
#########################################

# improvements to make
# don't allow country tags to be secondary tag of another country

import json
import os

packageFile = '../../working files/hdxMetaDataScrape_feb.json'

print('Loading file')
with open(packageFile) as json_file:
	packages = json.load(json_file)

with open('sample.json', 'w') as file:
    json.dump(packages[0:5], file)