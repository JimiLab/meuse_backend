"""
initializes files for the cron job
"""

import pickle

station_filename = "stations.pickle"
output_filename = "output.csv"
output_string = None
stationslist = pickle.load(open("stations.pickle"))

for station in stationslist:
	try:
		if output_string:
			output_string = output_string + ", " + station.decode('utf8')
		else:
			output_string = station.decode('utf8')
	except:
		if output_string:
			output_string = output_string + ", error" 
		else:
			output_string = "error"

output_file = open(output_filename, "w+")
output_file.write(output_string)

