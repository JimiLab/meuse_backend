"""
automatically runs every 10 mins and checks which 
songs each artist is playing. then it writes the output
to the file
"""
import sys
sys.path.insert(0, '/Users/meuse/Meuse/env/meuse/meuse/meuse')
import json
import pickle
from shoutcast_wrapper import ShoutcastWrapper

sr = ShoutcastWrapper()

pickle_filename = "/Users/meuse/Meuse/env/meuse/meuse/meuse/diversity/stations.pickle"
output_filename = "/Users/meuse/Meuse/env/meuse/meuse/meuse/diversity/output.csv"
output_string = None
check_on = True

if (check_on):
	stationslist = pickle.load(open("/Users/meuse/Meuse/env/meuse/meuse/meuse/diversity/stations.pickle"))
	counter = 0

	for station in stationslist:
		counter = counter + 1
		print "Downloading station number " + str(counter)

		#get the playing artist and add to line
		currentartist = sr.getCurrentTrackForStation(station)

		#append the artist to the string
		try:
			if output_string:
				output_string = output_string + ", " + currentartist.decode('utf8')
			else:
				output_string = "\n" + currentartist.decode('utf8')
		except:
			if output_string:
				output_string = output_string + ", error" 
			else:
				output_string = "\n error"

	#append the string to the file
	with open(output_filename, "a") as myfile:
	    myfile.write(output_string)
