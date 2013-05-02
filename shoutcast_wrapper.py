"""
wrapper for shoutcast api
"""
import urllib2
import json
import requests

class ShoutcastWrapper:
	"""
	wrapper for accessing the shoutcast api
	"""
	rootUrl = "http://api.shoutcast.com/station/"
	nowPlayingUrl = "http://api.shoutcast.com/station/nowplaying"
	apiKey = "**api_key**"
	header = {'User-agent' : 'meuse'}

	def getStationPlayingArtist(self, artist):
		"""
		returns a list of shoutcast stations playing the given
		artist. Each station is a dictionary.

		parameters
		----------
		artist: the name of an artist

		return
		------
		a list of tuples (station name, listen count)
		"""

		stationDictList = []
		stationList = []

		args = {"ct" : artist, "f" : "json", "k" : self.apiKey}

		try:
			request = requests.get(self.nowPlayingUrl, params=args)
			data = request.json()

			#construct a list with the stations
			stationDictList = data['response']['data']['stationlist']['station']

			for item in stationDictList:
				stationList.append((item['name'], item['lc']))


		except Exception as e:
			print "No artists found OR http request error"
			stationList = []

		return stationList
