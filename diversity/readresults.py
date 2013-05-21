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
	print str(len(data[0])) + ", "+ str(column)
	output = []
	
	for row in data:
		if len(row) - 1>= column:
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

		prevelement = ""
		
		#display intersection with seed artist
		for element in datarow:
			
			print seedartist + ", " + element
			
			if (re.match(seedartistpattern, element)):
				seedartistcount = seedartistcount + 1

			elif (len(element.strip()) == 0):
				print "remove"
				totalcount = totalcount - 1

			if (element != prevelement):
				totalcount = totalcount + 1
				prevelement = element
			
			else:
				print "remove"

		#raw_input()

		prevpe = ""
		
		#display intersection with predictedartist
		for element in datarow:
			for predictedartist in predictedartists:
				if predictedartist != [] and len(predictedartist) > 1 :
					try:
						predictedartistpattern = ".*" + predictedartist[0] + ".*"
						if (re.match(predictedartistpattern, element)):
							predictedartistcount = predictedartistcount + 1
					except:
						print "Error"
			prevpe = element

		prevnpe = ""
		
		#display intersection with nonpredictedartist
		for element in datarow:
			
			for predictedartist in nonpredictedartists:
				if predictedartist != [] and element != prevnpe:
					try:
						predictedartistpattern = ".*" + predictedartist[0] + ".*"
						if (re.match(predictedartistpattern, element)):
							nonpredictedartistcount = nonpredictedartistcount + 1
					except:
						print "Error"
			prevnpe = element

	print "seedartist: " + str(seedartistcount) + ", " + str((float(seedartistcount)/totalcount))
	print "predictedartist: " + str(predictedartistcount) + ", " + str((float(predictedartistcount) / 3.0))
	print "nonpredictedartist: " + str(nonpredictedartistcount) + ", " + str((float(nonpredictedartistcount) / 6.0))
	print "other: " + str((totalcount - seedartistcount - predictedartistcount))
	print "total: " + str(totalcount)

if  __name__ =='__main__':main()

