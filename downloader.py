"""
downloads the set of artists from lastfm and adds them
to the database
"""
import database_wrapper
import lastfm_wrapper

class Downloader:
	"""
	downloads data to database. Works between
	the web api wrappers and the database wrapper
	"""

	def download_artists():
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

	def download_artist_popularity(self):
		"""
		downloads the listen count for each artist
		from last.fm and adds log listen count into 
		the popularity field in the database
		"""
		lfr = lastfm_wrapper.LastFMWrapper()
		dbwr = database_wrapper.DatabaseWrapper()

		artists = dbwr.getArtists()

		for artist in artists:
			print artist

