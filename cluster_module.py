"""
conducts the clustering function using scikit-learn
"""

from database_wrapper import DatabaseWrapper
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction import FeatureHasher
from sklearn.cluster import KMeans
from shoutcast_wrapper import ShoutcastWrapper
class ClusterModule:
	"""
	clusters stations by artist using scikit-learn.
	"""
	numberOfClusters = 3

	def cluster(self, dataset):
		"""
		clusters the data provided into the number of 
		clusters set by self.numberOfClusters

		stationToArtist: dict where 
		dict['data'] = data in a 2d array
		dict['labels'] = labels for each array

		returns
		-------
		a list of clusters, where a cluster is a
		list of station names
		"""
		outputlabels = [] #the set of stations per cluster
		outputdata = [] #list of set of artists per cluster
		finaloutputdata = [] 

		hasher = FeatureHasher(input_type="string")
		transformer = TfidfTransformer()
		km = KMeans(n_clusters=self.numberOfClusters, init='k-means++',
			max_iter=10, n_init=1, verbose=0)

		#datacounts = counter.fit_transform(dataset['data'])
		datacounts = hasher.fit_transform(dataset['data'])
		#tfidfcounts = transformer.fit_transform(datacounts)
		
		#disabled tf-idf because too slow
		#km.fit(tfidfcounts)
		km.fit(datacounts)
		
		labeleddata = km.labels_

		#init output array
		for i in range(0, len(set(labeleddata))):
			outputlabels.append([])
			outputdata.append([])

		#add items to output array
		for i in range (0, len(labeleddata)):
			currentcluster = labeleddata[i]
			outputlabels[currentcluster].append(dataset['labels'][i])
			outputdata[currentcluster].append(dataset['data'][i])

		#change the artist list to artist sets
		for item in outputdata:
			listofartists = []

			for artistlist in item:
				for artist in artistlist:
					listofartists.append(artist)

			finaloutputdata.append(list(set(listofartists)))

		return {"labels" : outputlabels, "data" : finaloutputdata}

	def getPlayingStations(self, artist):
		"""
		gets the playing stations from last.fm and gets the 
		stations who have historically played the artist from
		the database.
		Then, merges the sets and gets the artists for each
		station.

		Returns a dictionary with 2 lists. 1 is a list of stations
		as 3-tuples, the other is the list of artist for each
		station

		parameters
		----------
		artist: name of the artist being played
		dataset: dataset of stations who have played the artist.
			dictionary with two keys: 
			'labels' contains the set of station 3-tuples
			'data' contains the list of artists for each station
		"""
		sr = ShoutcastWrapper()
		db = DatabaseWrapper()
		mergedict = {}
		mergelist = []
		artistsetlist = []

		#gets the set of currently playing stations
		playingStationSet = sr.getStationPlayingArtist(artist)
		
		#gets the set of historically played stations
		historicalStationSet = db.getStationTuplesForArtist(artist) 

		#merges the two sets of stations, while preserving order of
		#listen count
	
		#add all of the historically played stations
		itemcount = 0
		for item in historicalStationSet:
			itemId = item[1]
			itemName = item[0]

			mergedict[itemId] = itemcount
			mergelist.append((itemId, itemName, False))
			itemcount = itemcount + 1

		#add only the unique stations from now playing
		for item in playingStationSet:
			itemId = item[2]
			itemName = item[0]
			itemLC = item[1]

			#if the station is already in the list, change
			#status to playing
			if (mergedict.has_key(itemId)):
				itemnumber = mergedict[itemId]
				mergelist[itemnumber] = (itemId, itemName, True)

			#else append the station to the top of the list
			#and add the station to the db
			else:
				mergelist.insert(0, (itemId, itemName, True))
				db.addStationForArtist(artist, (itemName, itemId, itemLC)) 

		#get set of artists for each station
		for item in mergelist:
			stationID = item[0]
			artistset = db.getArtistsForStationID(stationID)
			artistsetlist.append(artistset)

		return {"data" : artistsetlist, "labels" : mergelist}
	
	def getJSONResponseForArtist(self, artist):
		"""
		creates a JSON response for a given artist
		
		parameters
		----------
		artist: name of artist to search for
		
		returns
		-------
		a JSON string consisting of:
			a list of 3 stations.
			each station contains:
				a representative artist
				3 tags representing the station
		"""
		
		#get the data for the artist
		dataset = self.getPlayingStations(artist)

		#cluster the data
		clusteredset = self.cluster(dataset)

		#pick the station for each set


		return clusteredset
def main():
	#get dataset for artist sting
	db = DatabaseWrapper()
	cluster = ClusterModule()

	dataset = db.getStationSetForArtist("Coldplay")
	output = cluster.cluster(dataset)

	print(output)

if  __name__ =='__main__':main()
