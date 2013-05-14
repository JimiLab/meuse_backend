"""
initializes the first several lines of a csv file. The first line contains the seed artist, and the second line has the current artist
"""

import pickle

picklefile = "artistsandstations.pickle"
outputfile = "firstlines.csv"
stationpicklefile = "stations.pickle"

#open the station pickle file
stationpickle = pickle.load(open(stationpicklefile, "r+"))

#load the pickle file
picklecontents = pickle.load(open(picklefile, "r+"))

line1 = ""
line2 = ""
line3 = ""
line4 = ""
line5 = ""
line6 = ""

data = None

#get first line
for seedartist in picklecontents:
	artistline = picklecontents[seedartist]

	for station in artistline:
		
		stationline = artistline[station]
	
		counter = 0
		
		for artist in stationline:
			print artist
			counter = counter + 1

			#print item
			line1 = line1 + ", " + seedartist
			line2 = line2 + ", " + station
			line3 = line3 + ", " + artist[0]
			line4 = line4 + ", " + stationline[0][0]
			
			if (len(stationline) > 1):
				line5 = line5 + ", " + stationline[1][0]
			if (len(stationline) > 2):
				line6 = line6 + ", " + stationline[2][0]


		while (counter < 3):
			print ""
			counter = counter + 1

			#print item
			line1 = line1 + ", " + seedartist
			line2 = line2 + ", " + station
			line3 = line3 + ", "
			line4 = line4 + ", " 
			line5 = line5 + ", " 
			line6 = line6 + ", " 

line1 = line1[2:]
print line1

line2 = line2[2:]
print line2

line3 = line3[2:]
print line3

line4 = line4[2:]
print line3

line5 = line5[2:]
print line3

line6 = line6[2:]
print line3


#write to output
#append the string to the file
with open(outputfile, "a") as myfile:
    myfile.write((line1 + '\n' \
    + line2 + '\n' \
    + line3 + '\n' \
    + line4 + '\n' \
    + line5 + '\n' \
    + line6 + '\n').encode('utf-8'))
