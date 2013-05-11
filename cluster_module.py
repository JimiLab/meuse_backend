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
		output = []
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
			output.append([])

		#add items to output array
		for i in range (0, len(labeleddata)):
			currentcluster = labeleddata[i]
			output[currentcluster].append(dataset['labels'][i])

		return output

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

		#gets the set of currently playing stations
		playingStationSet = sr.getStationPlayingArtist(artist)
		
		#gets the set of historically played stations
		historicalStationSet = db.getStationTuplesForArtist(artist) 

		#merges the two sets of stations, while preserving order of
		#listen count
		
		#add all of the playingstationset in order id, name, isplaying
		for item in playingStationSet:
			mergedict[(item[2], item[0])] = None
			mergelist.append((item[2], item[0], True))
	
		#add only the unique stations from the historically played stations
		for item in historicalStationSet:
			itemId = item[1]
			itemName = item[0]
			
			if not mergedict.has_key((itemId, itemName)):
				mergelist.append((itemId, itemName, False))	
		

		return mergelist 


def main():
	#get dataset for artist sting
	db = DatabaseWrapper()
	cluster = ClusterModule()

	dataset = db.getStationSetForArtist("Coldplay")
	output = cluster.cluster(dataset)

	print(output)

if  __name__ =='__main__':main()
