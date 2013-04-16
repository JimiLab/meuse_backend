"""
A wrapper for the pylast framework
"""
import pylast

class PylastWrapper:
	"""
	a wrapper for the pylast framework
	"""
	# You have to have your own unique two values for API_KEY and API_SECRET
	# Obtain yours from http://www.last.fm/api/account for Last.fm
	API_KEY = "**api_key**" # this is a sample key
	API_SECRET = "**api_key**"

	# In order to perform a write operation you need to authenticate yourself
	username = ""
	password_hash = pylast.md5("")

	# The number of tags to retrieve when getTags is called
	numberOfTags = 500

	def __init__(self):
		self.network = pylast.LastFMNetwork(api_key = self.API_KEY, api_secret = 
		    self.API_SECRET, username = self.username, password_hash = self.password_hash)

	def getTags(self):
		"""
		returns a number of top tags. The number is set by self.numberOfTags
		tags are returned in list form
		"""
		tags = self.network.get_top_tags(self.numberOfTags)
		output = []

		#clean up the tags by returning only all of the tags not the objects
		for tag in tags:
			output.append(tag[0].get_name())

		return output

	def topArtistForTag(self, artistIn):
		"""
		returns the top x tags for the given artistIn
		"""
		


