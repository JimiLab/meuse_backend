"""
conducts the clustering function using scikit-learn
"""
import json
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
		
		#edit the dataset so that it contains only artist name and not
		#artist popularity
		artistdataset = dataset['data']
		newartistdataset = []
		for i in range(0, len(artistdataset)):
			newartistdataset.append(artistdataset[i][0][0])
		
		print "clustering " + str(len(artistdataset))  + " artists"

		datacounts = hasher.fit_transform(newartistdataset)
		#tfidfcounts = transformer.fit_transform(datacounts)
		
		#disabled tf-idf because too slow
		#km.fit(tfidfcounts)
		print "clustering"
		km.fit(datacounts)
		print "clustering done"

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
		
		print playingStationSet

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
			itemCT = item[3]

			#if the station is already in the list, change
			#status to playing
			if (mergedict.has_key(itemId)):
				itemnumber = mergedict[itemId]
				mergelist[itemnumber] = item 
			#else append the station to the top of the list
			#and add the station to the db
			else:
				mergelist.insert(0, (itemId, itemName, True, itemCT))
				db.addStationForArtist(artist, (itemName, itemId, itemLC)) 

		#get set of artists for each station
		for item in mergelist:
			stationID = item[0]
			artistset = db.getArtistsForStationID(stationID)
			artistsetlist.append(artistset)

		return {"data" : artistsetlist, "labels" : mergelist}

	def selectTopStationTags(self, data):
		"""
		selects top tags when given a list of stations
		
		parameters
		----------
		data: a list of artists
		
		returns: top 3 tags for each artist
		"""
		db = DatabaseWrapper()
		taglist = []
		output = []

		for station in data:
			topTags = db.getTagsForStation(station[0])
			taglist.append(topTags)
		
		if (len(taglist) > 2):
			#calculate set differences
			output.append(list(set(taglist[0]) - (set (taglist[1] + taglist[2])))[:3]) 
			#if the first set difference is empty, just return the first three tags
			if (output[0] == []):
				output[0] = taglist[0][:3]

			output.append(list(set(taglist[1]) - (set (output[0] + taglist[2])))[:3]) 
			output.append(list(set(taglist[2]) - (set (output[1] + output[0])))[:3]) 
		else:
			print "Not enough tags to calculate tag difference"
			output = [[],[],[]]

		return output

	def selectRepresentativeArtists(self, data):
		"""
		selects a representative set of artists using the set difference
		
		parameters
		----------
		data: the dataset of 3 clusters of artist sets
		"""
		setlist = [] #list of unique sets
		differencelist = [] #list of set differences
		outputlist = []

		#make sure there is enough data
		if (len(data) > 2):
		#calculate set differences
			setlist.append(list(set(data[0]) - (set (data[1] + data[2])))) 
			setlist.append(list(set(data[1]) - (set (data[0] + data[2])))) 
			setlist.append(list(set(data[2]) - (set (data[1] + data[0])))) 
		else:
			print "Not enough artists to recommend"
			setlist = [[],[],[]]

		#sort the lists in order of popularity
		for item in setlist:
			outputlist.append(sorted(item, key=lambda tup: tup[1], reverse=True)[:3])
		
		return outputlist
	
	def getDataResponseForArtist(self, artist):
		"""
		creates a response for a given artist but not in JSON
		
		parameters
		----------
		artist: name of artist to search for
		
		returns
		-------
		a list of 3 stations.
		each station contains:
			a representative artist
			3 tags representing the station
		"""
		#vars
		sr = ShoutcastWrapper()
		topartists = []
		topstations = []
		toptags = []
		
		outputlist = []

		#get the data for the artist
		dataset = self.getPlayingStations(artist)
		
		#error check - if no artists
		if len(dataset['data']) < 4:
			return []

		#cluster the data
		clusteredset = self.cluster(dataset)

		#pick the station for each set
		stations = clusteredset['labels']
		for item in stations:
			stationtoappend = item[0]

			#check if there is a now playing artist
			if (stationtoappend[2] == False):
				shoutcastid = stationtoappend[0]
				name = stationtoappend[1]
				currenttrack = sr.getCurrentTrackForStationWithData(name)
				currenttrackname = currenttrack['stationname']
				bitrate = currenttrack['br']
				encoding = currenttrack['en']
				newstationtuple = (shoutcastid, name, False, currenttrackname, bitrate, encoding)
				topstations.append(newstationtuple)
			else:
				topstations.append(stationtoappend)

		
		#append 3 dummy lists to topstations to prevent errors if no stations found
		for i in range(0,3):
			topstations.append(("",""))

		#pick the representative artist for each set
		topartists = self.selectRepresentativeArtists(clusteredset['data'])

		#pick the top tags for each station
		toptags = self.selectTopStationTags(topstations)

		for i in range(0,3):
			outputlist.append([topstations[i], topartists[i], toptags[i]])

		return outputlist

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
		#vars
		sr = ShoutcastWrapper()
		topartists = []
		topstations = []
		toptags = []
		
		outputlist = []

		#get the data for the artist
		dataset = self.getPlayingStations(artist)
		
		#error check - if no artists
		if len(dataset['data']) < 4:
			return json.dumps({"success" : "False", "data" : []})
		
		print "clustering"

		#cluster the data
		clusteredset = self.cluster(dataset)
		
		print "clustering done"

		#pick the station for each set
		stations = clusteredset['labels']
		for item in stations:
			stationtoappend = item[0]

			#check if there is a now playing artist
			if (stationtoappend[2] == False):
				shoutcastid = stationtoappend[0]
				name = stationtoappend[1]
				currenttrack = sr.getCurrentTrackForStationWithData(name)
				currenttrackname = currenttrack['stationname']
				bitrate = currenttrack['br']
				encoding = currenttrack['en']
				listencount = currenttrack['lc']
				newstationtuple = (shoutcastid, name, False, currenttrackname, bitrate, encoding, listencount)

				topstations.append(newstationtuple)
			else:
				topstations.append(stationtoappend)

		
		#append 3 dummy lists to topstations to prevent errors if no stations found
		for i in range(0,3):
			topstations.append(("",""))

		#pick the representative artist for each set
		topartists = self.selectRepresentativeArtists(clusteredset['data'])

		#pick the top tags for each station
		toptags = self.selectTopStationTags(topstations)

		for i in range(0,3):
			outputlist.append([topstations[i], topartists[i], toptags[i]])

		return json.dumps({"success":"True", "data":outputlist})
		
def main():
	#get dataset for artist sting
	db = DatabaseWrapper()
	cluster = ClusterModule()

	dataset = db.getStationSetForArtist("Coldplay")
	output = cluster.cluster(dataset)

	print(output)

if  __name__ =='__main__':main()

