"""
for the given list, gets the stations for a given list of artists,
then records the stations and the predicted artists in a machine-readable
file
"""
import sys
sys.path.insert(0, '/Users/meuse/Meuse/env/meuse/meuse/meuse')
import json
import pickle
from cluster_module import ClusterModule
from database_wrapper import DatabaseWrapper
from random import shuffle

cm = ClusterModule()
db = DatabaseWrapper()

stations_output_file = "stations.pickle"
artists_output_file = "artistsandstations.pickle"
stationslist = []
stationstoartist = {} #dict where stations are mapped to predicted and seed artists
artistsforstation = []
artistsforotherstations = []

#define a list of artists, select 100 from db
allartists = db.get100ArtistFromTop500()

#select 100 artists randomly
#shuffle(allartists)

artist_list = allartists[:100]

#reverse artist list
artist_list.reverse()

counter = 0

for seedartist in artist_list:
	counter = counter + 1

	print "artist number " + str(counter)

	response = cm.getDataResponseForArtist(seedartist)
	
	#get the recommended stations for the seed artist

	for i in range (0, len(response)):

		stationname = response[i][0][1]

		#get the predicted artist for the station
		stationslist.append(stationname)
	
		artistsforstation = response[i][1]
		
		#get the predicted artists for the other 2 stations
		artistsforotherstations = []

		for j in range(0, len(response)):
			if (j != i):
				print str(i) + ", " + str(j)
				listofartists = response[j][1]
				print artistsforotherstations
				for artist in listofartists:
					artistsforotherstations.append(artist)

		stationstoartist[stationname] = {"predicted" : artistsforstation, "notpredicted" : artistsforotherstations, "seedartist" : seedartist}


#serialize both lists to pickles
pickle.dump(stationstoartist, open(artists_output_file, "w+")) 
pickle.dump(stationslist, open(stations_output_file, "w+")) 

		
