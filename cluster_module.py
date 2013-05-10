"""
conducts the clustering function using scikit-learn
"""

from database_wrapper import DatabaseWrapper
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction import FeatureHasher
from sklearn.cluster import KMeans

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


def main():
	#get dataset for artist sting
	db = DatabaseWrapper()
	cluster = ClusterModule()

	dataset = db.getStationSetForArtist("Coldplay")
	cluster.cluster(dataset)


if  __name__ =='__main__':main()
