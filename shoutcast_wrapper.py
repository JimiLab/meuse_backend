"""
wrapper for shoutcast api
"""
import urllib2
import json
import requests
import xml.etree.cElementTree as et

class ShoutcastWrapper:
	"""
	wrapper for accessing the shoutcast api
	"""
	rootUrl = "http://api.shoutcast.com/station/"
	nowPlayingUrl = "http://api.shoutcast.com/station/nowplaying"
	currentTrackUrl = "http://api.shoutcast.com/legacy/stationsearch"
	apiKey = "**api_key**"
	header = {'User-agent' : 'meuse'}

	def getCurrentTrackForStation(self, station):
		"""
		looks up what a station is playing

		parameters
		----------
		station: the name of the station to check
		
		return
		------
		track being played by the station
		"""

		args = {"search" : station, "k" : self.apiKey, "f" : "json"}

		try:
			request = requests.get(self.currentTrackUrl, params=args)
			data = request.content

			#parse the xml data
			tree = et.fromstring(data)
			person = tree.find('station')
			stationname = person.attrib.get('ct') 

		except Exception as e:
			print "No artists found OR http request error"
			return ""

		return stationname

	def getStationPlayingArtist(self, artist):
		"""
		returns a list of shoutcast stations playing the given
		artist. Each station is a three-tuple.

		parameters
		----------
		artist: the name of an artist

		return
		------
		a list of tuples (station name, listen count, id)
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
				stationList.append((item['name'], item['lc'], item['id'], item['ct']))


		except Exception as e:
			print "No artists found OR http request error"
			stationList = []

		return stationList
