"""
reads the results of the recorded data
"""
import pickle
import re

def readcsvfile():
	"""
	reads the csv file containing the station data
	and returns it as a 2d list
	"""
	rawstationdata_filename = "output.csv"

	#load the .csv file
	stationsdata_raw = open(rawstationdata_filename, "r+")

	stationsdata_processed = []
	
	#read into list of raw lines
	stationsdata = stationsdata_raw.readlines()

	#break into list of station names
	for line in stationsdata:
		tracks = line.split(",")
		
		stationsdata_processed.append(tracks)

	return stationsdata_processed

def getcolumnfromdata(data, column):
	output = []
	
	for row in data:
		if len(row) > 2:
			output.append(row[column])
	
	return output
	
def main():
	stationpickle_filename = "stations.pickle"
	artistsandstations_filename = "artistsandstations.pickle"

	#load both pickles
	a2s = pickle.load(open(artistsandstations_filename, "r+"))
	stations = pickle.load(open(stationpickle_filename, "r+"))

	stationdata = readcsvfile()

	#accumulators
	seedartistcount = 0
	totalcount = 0
	predictedartistcount = 0
	nonpredictedartistcount = 0

	#get data for each station
	for i in range(0, len(stations)):
		station = stations[i]
		currentstationdata = a2s[station]
		
		predictedartists = currentstationdata['predicted']
		nonpredictedartists = currentstationdata['notpredicted']
		seedartist = currentstationdata['seedartist']

		datarow = getcolumnfromdata(stationdata, i)

		#search for seedartist
		seedartistpattern = ".*" + seedartist + ".*" 

		#display intersection with seed artist
		for element in datarow:
			if (re.match(seedartistpattern, element)):
				print seedartist + ", " + element
				seedartistcount = seedartistcount + 1

		#display intersection with predictedartist
		for element in datarow:
			for predictedartist in predictedartists:
				predictedartistpattern = ".*" + predictedartist[0] + ".*"
				if (re.match(predictedartistpattern, element)):
					predictedartistcount = predictedartistcount + 1
		#display intersection with nonpredictedartist
		for element in datarow:
			for predictedartist in nonpredictedartists:
				predictedartistpattern = ".*" + predictedartist[0] + ".*"
				if (re.match(predictedartistpattern, element)):
					nonpredictedartistcount = nonpredictedartistcount + 1


		totalcount = totalcount + 1
			
	print "seedartist: " + str(seedartistcount)
	print "predictedartist: " + str(predictedartistcount)
	print "nonpredictedartist: " + str(nonpredictedartistcount)
	print "other: " + str((totalcount - seedartistcount - predictedartistcount))

if  __name__ =='__main__':main()
