"""
downloads the set of artists from lastfm and adds them
to the database
"""
import database_wrapper
import lastfm_wrapper
import shoutcast_wrapper
import pickle

class Downloader:
	"""
	downloads data to database. Works between
	the web api wrappers and the database wrapper
	"""

	def download_similar_artists(self):
		"""
		for each of the artists in the database
		downloads all the similar artists
		and adds entries in a2a database
		"""
		lfr = lastfm_wrapper.LastFMWrapper()
		dbwr = database_wrapper.DatabaseWrapper()

		artistSet = dbwr.getArtistsInDescendingOrder()
		similarArtistSet = []

		artistID = 0
		similarArtistID = 0
		similarArtistName = ""
		similarArtistScore = 0.0
		count = 0

		for artist in artistSet:
			count = count + 1

			print "Adding similar artists for " + artist + ", artist " \
			+ str(count) + " of " + str(len(artistSet))

			artistID = dbwr.getArtistID(artist)

			#download the similar artist set
			similarArtistSet = lfr.getSimilarArtists(artist)

			for similarArtist in similarArtistSet:
				print similarArtist

				#add each similar artist to the database

				similarArtistName = similarArtist[0]
				similarArtistScore = similarArtist[1]

				if (dbwr.checkIfArtistExists(similarArtistName) != 0):
					#add artist details ONLY if it exists.
					similarArtistID = dbwr.getArtistID(similarArtistName)

					#check to see if the a2a entry exists
					if (dbwr.checkIfA2AExists(artistID, similarArtistID)==0):
						dbwr.addArtistToA2A(artistID, similarArtistID, similarArtistScore)

					else:
						dbwr.updateA2A(artistID, similarArtistID, similarArtistScore)


	def download_artists(self):
		#get the set of artists twice
		artists = []

		lfr = lastfm_wrapper.LastFMWrapper()
		dbwr = database_wrapper.DatabaseWrapper()

		for i in range(1, 3):
			print ""
			print "Downloading artists - iteration " + str(i)
			print "----------------------------------"

			tempArtists = lfr.getArtistSet()
			artists = artists + tempArtists
			artists = list(set(artists))


		print "Download complete, " + str(len(artists)) + " downloaded"

		#put artists in db
		dbwr.addArtists(artists)

		print "Artists succesfully added to database"

	def update_artist_popularity(self):
		"""
		updates the artist popularity. 
		popularity is a moving average 

		popularity = (old popularity + new popularity)/2
		"""
		lfr = lastfm_wrapper.LastFMWrapper()
		dbwr = database_wrapper.DatabaseWrapper()

		artists = dbwr.getArtists()

		counter = 0

		artistDict = {}

		print "downloading popularity data for " + str(len(artists)) + " artists"

		for artist in artists:
			counter = counter + 1

			print "Getting data for artist " + str(counter) + ", " + artist

			#get popularity
			popularity = lfr.getLogListenCount(artist)

			dbwr.updateArtistPopularity(artist, popularity)

		print "All entries added to database"

	def download_tags(self):
		"""
		downloads all tags relevant to each artist
		and stores it in the tags table. Adds connections
		to the a2t table
		"""
		lfr = lastfm_wrapper.LastFMWrapper()
		dbwr = database_wrapper.DatabaseWrapper()

		artistsAndTags = {}
		counter = 0

		#get list of artists
		artists = dbwr.getArtists()

		print "There are " + str(len(artists)) + " artists"

		#for each artist, get tags 
		for artist in artists:
			artistsAndTags = {}

			counter = counter + 1

			print "Downloading tags for artist " + \
			str(counter) + ", " + artist

			tags = lfr.getArtistTags(artist)

			artistsAndTags[artist] = tags

			dbwr.addTags(artistsAndTags)

		#dump artist to pickle
		#pickle.dump(artistsAndTags, open("artistTags.pickle", "wr"))

		#add to a2t, tags
		#dbwr.addTags(artistsAndTags)

	def download_stations(self):
		"""
		downloads all the stations to the database
		stations are found by, for each artist, checking
		the stations playing that artist and adding that
		station for that artist

		each time this is run, it increments scores for 
		a station + artist combination by 1 if that stations
		is playing a song by that artist
		"""
		#vars
		dbwr = database_wrapper.DatabaseWrapper()
		lfr = lastfm_wrapper.LastFMWrapper()
		shr = shoutcast_wrapper.ShoutcastWrapper()

		artistsAndStations = {}

		counter = 0

		#get all the artists
		artists = dbwr.getArtists()

		#for each artist, get the set of playing stations
		for artist in artists:
			artistsAndStations = {}

			counter = counter + 1

			print "Downloading stations for artist " + str(counter) + \
				" of " + str(len(artists)) + " : " + artist

			stationsForArtist = shr.getStationPlayingArtist(artist)

			artistsAndStations[artist] = stationsForArtist

			#add the stations to the database
			dbwr.addArtistsToStation(artistsAndStations)

		print "all done!"

def main():
	downloader = Downloader()
	downloader.download_similar_artists()

if  __name__ =='__main__':main()
