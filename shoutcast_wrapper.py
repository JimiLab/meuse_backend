"""
wrapper for shoutcast api
"""
import urllib2
import json

class ShoutcastWrapper:
	"""
	wrapper for accessing the shoutcast api
	"""
	rootUrl = "http://api.shoutcast.com/station/"
	apiKey = "**api_key**"
	header = {'User-agent' : 'meuse'}

	def getStationPlayingArtist(self, artist):
		"""
		returns a list of shoutcast stations playing the given
		artist. Each station is a dictionary.
		"""

		output = []
		url = self.rootUrl + "nowplaying?ct=" + artist + "&f=json" + "&k=" + self.apiKey
		req = urllib2.Request(url, None, self.header)
		html = urllib2.urlopen(req).read()

		jsondata = json.loads(html)

		for tag in jsondata['response']['data']['stationlist']['station']:
			output.append(tag)

		return output
