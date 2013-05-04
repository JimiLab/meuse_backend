"""
last.fm wrapper for python lastfm.  
"""
import urllib2
import json
import time
import requests
import math

class LastFMWrapper:
	"""
	wrapper for last.fm api in python
	"""
	rootUrl = "http://ws.audioscrobbler.com/2.0/"	
	apiKey = "**api_key**" 
	header = {'User-agent' : 'meuse'}
	numberOfTags = 100
	numberOfArtists = 100
	sleeptime = 2

	def getLogListenCount(self, artist):
		"""
		gets the log listed count for artist
		on error, sets listen count to 0
		"""

		try:
			listenCount = int(self.getArtistInfo(artist)['stats']['listeners'])

		except:
			print "Connection error, setting log listen count to 0"
			listenCount = 1

		finally:
			print str(listenCount)
			output = math.log(listenCount, 2)

			return output

	def getSimilarArtists(self, artist):
		"""
		gets the similar artist for the given artist; returns a 
		list of tuples, where artist is mapped to score

		parameters
		----------
		artist - name of the artist
		"""
		output = []
		jsondata = []

		args = {"method" : "artist.getsimilar", 
		"artist" : artist, "api_key" : self.apiKey,
		"format" : "json"}

		try:
			data = requests.get(self.rootUrl, params=args)
		
			jsondata = json.loads(data.text)

			jsondata = jsondata['similarartists']['artist']

			for tag in jsondata:
				output.append(((tag['name']), float(tag['match'])))

		except:
			"print json value error!"

		finally:
			return output

	def getArtistTags(self, artist):
		"""
		gets the tags for the given artist; returns a list
		"""
		output = []
		jsondata = []

		args = {"method" : "artist.gettoptags", 
		"artist" : artist, "api_key" : self.apiKey,
		"format" : "json"}

		try:
			data = requests.get(self.rootUrl, params=args)
		
			jsondata = json.loads(data.text)

			jsondata = jsondata['toptags']['tag']

			for tag in jsondata:
				output.append(tag['name'])

		except:
			"print json value error!"

		finally:
			return output


	def getArtistInfo(self, artist):
		"""
		returns the 'info' for a given artist
		throws connectionError
		"""
		output = {}

		args = {"method" : "artist.getinfo", 
		"artist" : artist, "api_key" : self.apiKey,
		"format" : "json"}

		try:
			data = requests.get(self.rootUrl, params=args)
		
			jsondata = json.loads(data.text)

			output = jsondata['artist']

		except:
			"print json value error!"

			#hard code 0 for listen count
			output['stats'] = {}
			output['stats']['listeners'] = 1

		finally:
			return output

	def getSimilarArtist(self, artist):
		"""
		returns a list of artists similar to the artist
		passed in
		"""
		output = []
		url = self.rootUrl + "?method=artist.getsimilar&artist=" + artist + "&api_key=" + self.apiKey + "&format=json" + "&limit=" + str(self.numberOfArtists)
		req = urllib2.Request(url, None, self.header)
		html = urllib2.urlopen(req).read()

		jsondata = json.loads(html)

		for tag in jsondata['similarartists']['artist']:
			output.append(tag['name'])

		return output
		
	def getTopTags(self):
		"""
		gets the top 500 tags from lastfm and returns a list of tags
		"""
		output = []

		url = self.rootUrl + "?method=chart.getTopTags&api_key=" + self.apiKey + "&format=json" + "&limit=" + str(self.numberOfTags)

		req = urllib2.Request(url, None, self.header)
		html = urllib2.urlopen(req).read()

		jsondata = json.loads(html)

		for tag in jsondata['tags']['tag']:
			output.append(tag['name'])
		
		return output

	def getTopArtistsForTag(self, tagName):
		"""
		returns a list of top artists for a given tag
		"""
		output = []

		url = self.rootUrl + "?method=tag.gettopartists&tag=" + tagName +\
		 "&api_key=" + self.apiKey + "&format=json" + "&limit=" + str(self.numberOfArtists)
		req = urllib2.Request(url, None, self.header)
		html = urllib2.urlopen(req).read()

		jsondata = json.loads(html)

		for tag in jsondata['topartists']['artist']:
			output.append(tag['name'])

		return output

	def getArtistSet(self):
		"""
		returns a list of artists got from the top numberOfArtists artists
		from the top numberOfTags tags
		"""
		output = []
		outputdict = {}
		tagnumber = 0

		#get the list of tags
		tags = self.getTopTags()

		print "There are " + str(len(tags)) + " tags"

		#get the list of artists for each tag
		for tag in tags:

			#make it sleep for a bit, probably
			time.sleep(self.sleeptime)

			try:
				print "Tag " + str(tag) + " is tag number " + str(tagnumber)

				tagnumber += 1

				artistsForTag = self.getTopArtistsForTag(tag)

				for artist in artistsForTag:
					outputdict[artist] = ""


			except Exception, e:
				print "Error with httplib"

		output = outputdict.keys()

		return output
		
		
