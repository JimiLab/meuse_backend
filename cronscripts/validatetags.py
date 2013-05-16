"""
does quality control on the tags, checking which ones should be active
for a tag to be active it has to have at least a threshold level of 
artists linking to it
"""

import sys
sys.path.insert(0, '/Users/meuse/Meuse/env/meuse/meuse/meuse')
from database_wrapper import DatabaseWrapper

threshold = 5 #minimum number of artists linking to a tag to make it valid

db = DatabaseWrapper()

#get tag data from database
data = db.getA2TCount()
tagstoactivate = []

for element in data:
	count = element[2]
	name = element[1]
	tagid = element[0]

	if count>threshold:
		tagstoactivate.append(tagid)
		print name

for tagID in tagstoactivate:
	db.activateTag(tagID)

print tagstoactivate
