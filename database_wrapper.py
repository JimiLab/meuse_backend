"""
A wrapper for the sqlite database
"""

import sqlite3
import MySQLdb as mdb
import sys

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""
	database = "database/meuse.db"

	username = "cs205user"
	password = "ithaca"
	database = "meuse2"

	con = None
	cur = None

	def connect(self):
		"""
		connects to the mysql database
		"""
		try:
			self.con = mdb.connect("localhost", self.username, self.password, self.database, use_unicode=True)
			self.cur = self.con.cursor()

		except _mysql.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
	
	def disconnect(self):
		"""	
		disconnects from database
		"""	
		if (self.con):
			self.con.close()

	def addStationForArtist(self, artist, station):
		"""
		adds a station for the given artist
		parameters
		----------
		artist: name of the artist
		station: 3 tuple
			name of the station,
			id of the station
			listen count
		"""
		stationName = station[0]
		stationId = station[1]
		stationLC = station[2]
		
		# add station to db
		self.addStation(stationName, stationLC, stationId)

		stationID = self.getStationIDWithLFMId(stationId)

		artistID = self.getArtistID(artist)

		if (self.checkIfA2SExists(artistID, stationID)):
			self.updateA2S(artistID, stationID)

		else:
			# add A2S entry
			self.addArtistToA2S(artistID, stationID, 0)

	def getStationSetForArtist(self, artist):
		"""
		***TOO SLOW***

		gets a dataset for clustering for a given
		artist. The dataset is as follows:

		parameters
		----------
		artist - the artist whose stations to return

		returns
		-------
		dictionary of all the stations for the artists
		passed in. 

		"labels" = list of stations for each set
		"data" = list of artist for each station

		"""
		dataset = {"labels" : [], "data" : []}
		artists = []
		stations = self.getStationsForArtist(artist)

		for station in stations:
			artists = []

			#get data from dataset and convert into a long string
			artistsForStation = self.getArtistsForStation(station)

			for artist in artistsForStation:

				artists.append(artist)

			dataset["data"].append(artists)
			dataset["labels"].append(station)

		return dataset
	
	def getTagsForStation(self, lastfmID):
		"""
		gets the top tags for a station 

		parameters
		----------
		lastfmid: lastfm id of the station
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("SELECT tags.name\
			FROM tags\
			INNER JOIN a2t ON tags.id = a2t.tagid\
			INNER JOIN a2s ON a2t.artistid = a2s.artistid\
			INNER JOIN station ON a2s.stationid = station.id\
			WHERE station.lastfmid = %s\
			and tags.isActive=True", lastfmID)
		
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
			
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output


	def getTagsForArtist(self, artist):
		"""
		gets the list of top tags for the artist

		artist: the name of the artist
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("select tags.name \
				from artist, tags, a2t \
				where artist.name=%s and\
				tags.id = a2t.tagid and\
				artist.id = a2t.artistid", artist)
		
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
			
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output

	def getStationTuplesForArtist(self, artist):
		"""
		gets the list of stations which have
		played the given artist
		the stations are ordered by the score
		for the givne artist
	
		parameters
		----------
		artist: the name of the artist

		returns
		-------
		a list of tuples. Each tuple contains (name, lastfmid)
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("select station.name, station.lastfmid \
				from artist, station, a2s \
				where artist.name=%s and\
				station.id = a2s.stationid and\
				artist.id = a2s.artistid \
				order by a2s.score, station.popularity \
				limit 0, 30", artist)
		
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return data


	def getStationsForArtist(self, artist):
		"""
		gets the list of stations which have
		played the given artist

		artist: the name of the artist
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("select station.name \
				from artist, station, a2s \
				where artist.name=%s and\
				station.id = a2s.stationid and\
				artist.id = a2s.artistid \
				order by station.popularity", artist)
		
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
			
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output

	def getArtistsForStationID(self, stationID):
		"""
		gets the list of artists who have been 
		played on the given station

		parameters
		----------
		stationID: id of the station
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("select artist.name, a2s.score \
				from artist, station, a2s \
				where station.lastfmid=%s and\
				station.id = a2s.stationid and\
				artist.id = a2s.artistid \
				order by artist.popularity \
				limit 0, 30", stationID)

			data = self.cur.fetchall()
			output = data
			"""
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
			"""
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output


	def getArtistsForStation(self, station):
		"""
		gets the list of artists who have been 
		played on the given station

		parameters
		----------
		station: name of the station
		"""
		data = []
		output = []

		try:
			self.connect()
			self.cur.execute("select artist.name \
				from artist, station, a2s \
				where station.name=%s and\
				station.id = a2s.stationid and\
				artist.id = a2s.artistid", station)

			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
	
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output

	def addTags(self, artistsAndTags):
		"""
		adds tags to the database. updates tag table
		and the a2t table

		artistAndTags is a dict where 
		dict[artist] = list of tags
		"""

		artistID = 0
		tagID = 0
		a2tID = 0

		listOfTags = []

		for artist in artistsAndTags:
			
			#check if artist exists
			artistID = self.getArtistID(artist)
			if (artistID == 0):
				self.addArtists([artist])
				artistID = self.getArtistID(artist)

			listOfTags = artistsAndTags[artist]

			for tag in listOfTags:
				tagName = tag[0]
				tagScore = tag[1]

				tagID = self.getTagID(tagName)

				#if tag does not exist, add to tags
				if (tagID == 0):
					self.addTag(tagName)
					tagID = self.getTagID(tagName)
				
				#if a2t entry does not exist, add it
				if (self.checkIfA2TExists(artistID, tagID) == 0):
					self.addA2T(artistID, tagID, tagScore)
				else:
					self.updateA2T(artistID, tagID, tagScore)

	def checkIfStationExists(self, lastfmid):
		"""
		checks if a station exists in the database

		parameters
		----------
		lastfmid: the last fm id of the station
		"""		
		output = None

		try:
			self.connect()

			self.cur.execute("select id from station \
			where lastfmid = %s",
			lastfmid)
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0

			return output

		con = sqlite3.connect(self.database)
		data = []
		output = 0

	def checkIfArtistExists(self, artist):
		"""
		checks if the given artist exists

		parameters
		----------
		artist: name of the artist

		return
		------
		0 if there are no artists with the name
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from artist \
			where name = %s",
			artist)
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0

			return output

	def checkIfA2AExists(self, artistID, simartistID):
		"""
		checks if an a2A entry exists

		parameters
		----------
		artistID - id of the artist
		simartistID - id of the similar artist

		returns
		-------
		0 if the a2a entry does not exist, or the score
		if it does
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from a2a \
			where artist1ID = %s and artist2ID = %s",
			(artistID, simartistID))
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0

			return output

	def checkIfA2SExists(self, artistID, stationID):
		"""
		checks if an a2t entry exists
		
		parameters
		----------
		artistID: id of the artist
		stationID: id of the station
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from a2s \
			where artistID = %s and stationID = %s",
			(artistID, stationID))
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0

			return output


	def checkIfA2TExists(self, artistID, tagID):
		"""
		checks if an a2t entry exists
		
		parameters
		----------
		artistID: id of the artist
		tagID: id of the tag
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from a2t \
			where artistID = %s and tagID = %s",
			(artistID, tagID))
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0

			return output


	def addTag(self, tagName):
		"""
		inserts a tag into the database

		parameters
		----------
		tagname: name of the tag
		"""
		try:
			self.connect()

			self.cur.execute("insert into tags(id, name, isactive) \
			values (DEFAULT, %s, FALSE)",tagName)

		except (mdb.Error, UnicodeEncodeError) , e: 
			print "Error!"

		finally:

			self.disconnect()

		con = sqlite3.connect(self.database)

	def getTagID(self, tagName):
		"""
		returns the ID for a tag, or 0 if it does not
		exist

		parameters
		----------
		tagName: name of the tag
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from tags where name = %s",
			tagName)
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %s: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0
			return output


	def getArtistToStationScore(self, artist, station):
		"""
		gets the score of an artist in the A2S table
		
		parameters
		----------
		artistID: id of artist
		stationID: id of station
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select score from a2s \
			where artistID = %s and stationID = %s",
			(artist, station))
	
			data = self.cur.fetchone()

			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %s: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			if output == None:
				return 0
			return output

	def addArtistsToStation(self, artistsAndStations):
		"""
		populates station and artistToStation database. score
		for each artist starts at 0 and is incremented

		parameters
		----------
		artistToStation: a dictionary where artist is mapped
		to a list of (station, listencount)
		"""
		for artist in artistsAndStations:

			stations = artistsAndStations[artist]

			for stationTuple in stations:

				station = stationTuple[0]	
				popularity = stationTuple[1]
				lastfmid = stationTuple[2]

				#check if station exists
				if (self.checkIfStationExists(lastfmid)==0):
					#put station in to the list
					self.addStation(station, popularity, lastfmid)

				#otherwise, update the station popularity
				else:
					self.updateStationPopularity(station, popularity)

				#get station id
				stationID = self.getStationID(station)

				#get artist id
				artistID = self.getArtistID(artist)

				#get current station score
				score = self.getArtistToStationScore(artistID, stationID)

				if score == 0:
					#create new row
					self.addArtistToA2S(artistID, stationID, 1)

				else:
					#update row
					self.updateArtistToA2S(artistID, stationID, score + 1)

	def getStationIDWithLFMId(self, stationID):
		"""
		gets the id for the station passed in using the last fm id of station

		parameters
		----------
		stationID: lastfm id of the station
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from station where lastfmid = %s",
			stationID)
	
			data = self.cur.fetchone()

			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0
			return output
	def getStationID(self, station):
		"""
		gets the id for the station passed in

		parameters
		----------
		station: name of the station
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from station where name = %s",
			station)
	
			data = self.cur.fetchone()

			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0
			return output

	def getArtistID(self, artist):
		"""
		gets the id for the artist passed in
		station: name of the artist
		"""
		output = None

		try:
			self.connect()

			self.cur.execute("select id from artist where name = %s",
			artist)
	
			data = self.cur.fetchone()
			#turn data from tuples into list of items
			output = data[0]

		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()
			if output == None:
				return 0
			return output

	def updateTagPopularity(self, tagID, newPopularity):
		"""
		updates the popularity of a tag as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		tag: name of the tag
		newPopularity: popularity of the tag	
		"""

		try:
			self.connect()
			
			self.cur.execute("update tags \
			set popularity = ((tags.popularity + %s)/2) \
			where id = %s",(newPopularity, tagID))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()


	def updateStationPopularity(self, station, newPopularity):
		"""
		updates the popularity of a station as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		station: name of the station
		newPopularity: popularity of the station	
		"""

		try:
			self.connect()
			
			self.cur.execute("update station \
			set popularity = ((station.popularity + %s)/2) \
			where name = %s",
			(newPopularity, station))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	def activateTag(self, tagID):
		"""
		activates the tag whose id is passed
	 	in
		"""
		try:
			self.connect()
			
			self.cur.execute("update tags\
			set isActive = True\
			where id = %s", tagID)

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	
	
	def updateA2S(self, artistID, stationID):
		"""
		Updates the score of a a2s score as follows

		popularity = score = score + 1

		parameters
		----------
		artistID: id of the artist
		stationID: id of the station
		score: score for the a2s
		"""
		try:
			self.connect()
			
			self.cur.execute("update A2s \
			set score = (a2s.score + 1) \
			where artistID = %s and stationID =%s",
			(artistID, stationID))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()


	def updateA2T(self, artistID, tagID, score):
		"""
		Updates the score of a tag as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		artistID: id of the artist
		tagID: id of the tag
		score: score for the tag
		"""
		try:
			self.connect()
			
			self.cur.execute("update A2T \
			set score = ((a2t.score + %s) / 2) \
			where artistID = %s and tagID =%s",
			(score, artistID, tagID))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	def updateArtistPopularity(self, artist, newPopularity):
		"""
		updates the popularity of an artist as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		artist: name of the artist
		newPopularity: popularity of the station	
		"""
		try:
			self.connect()
			
			self.cur.execute("update artist \
			set popularity = ((artist.popularity + %s) / 2) \
			where name = %s",
			(newPopularity, artist))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	def updateA2A(self, artist1ID, artist2ID, score):
		"""
		updates a row of the artistToStation table with a new
		score.

		parameters
		----------
		artist1ID: id of the artist
		artist2ID: id of the similar artist
		score: new score for the relationship
		"""
		try:
			self.connect()
			
			self.cur.execute("update a2a \
			set score = %s \
			where artist1ID = %s and artist2ID = %s",
			(score, artist1ID, artist2ID))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	def updateArtistToA2S(self, artistID, stationID, score):
		"""
		updates a row of the artistToStation table with a new
		score.

		parameters
		----------
		artistID: id of the artist
		stationID: id of the 
		score: score for the relationship
		"""
		try:
			self.connect()
			
			self.cur.execute("update a2s \
			set score = %s \
			where artistID = %s and stationID = %s",
			(score, artistID, stationID))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:
			self.disconnect()

	def addA2T(self, artistID, tagID, score):
		"""
		adds an entry to the a2t table

		parameters
		----------
		artist: id of the artist
		tag: id of the tag
		score: score for the tag
		"""		
		try:
			self.connect()
			
			self.cur.execute("insert into A2T \
			(id, artistID, tagID, score) \
			values (DEFAULT, %s, %s, %s)", (artistID, tagID, score))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()


	def addArtistToA2A(self, artist1ID, artist2ID, score):
		"""
		adds a station into the artistToStation table.

		parameters
		----------
		artist1ID: id of the artist
		artist2ID: id of the similar artist
		score: score
		"""
		try:
			self.connect()
			
			self.cur.execute("insert into a2a \
			(artist1ID, artist2ID, score) \
			values (%s, %s, %s)", (artist1ID, artist2ID, score))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()

	def addArtistToA2S(self, artistID, stationID, score):
		"""
		adds a station into the artistToStation table.
		artistID: id of the artist
		stationID: id of the 
		score: score
		"""
		try:
			self.connect()
			
			self.cur.execute("insert into a2s \
			(artistID, stationID, score) \
			values (%s, %s, %s)", (artistID, stationID, score))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()

	def addStation(self, station, popularity, lastfmid):
		"""
		adds a station into the database. Turns into an update if it exists

		parameters
		----------
		station: the name of the station
		popularity: the listen count of the station
		lastfmid: the last.fm id of the station
		"""
		try:
			self.connect()

			self.cur.execute("insert into station(id, name, popularity, lastfmid) \
			values (DEFAULT, %s, %s, %s)",(station, popularity, lastfmid))

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()
	
	def addArtistPopularity(self, artists):
		"""
		adds the popularity of each artist into the 
		database.
		
		parameters
		----------
		artists: a dictionary where artists is mapped
		to popularity.
		"""
		for artist in artists:

			try:
				self.connect()

				self.cur.execute("update artist set popularity = (%s) \
				where name = (%s)", (artists[artist], artist))

			except mdb.Error, e: 
				print "Error!"
				print "Error %d: %s" % (e.args[0],e.args[1])

			finally:

				self.disconnect()

	def addArtist(self, artist):
		"""
		adds the artist into the database

		parameters
		----------
		artist: name of the artist
		"""
		try:
			self.connect()

			self.cur.execute("insert into artist(id, name, popularity) \
			values (DEFAULT, %s, 0)",artist)

		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()

	def addArtists(self, artists):
		"""
		adds a list of artists into the database
		inits popularity at 0

		parameters
		----------
		artists: a list of artists
		"""
		try:
			self.connect()

			for artist in artists:
				self.cur.execute("insert into artist(id, name, popularity) \
				values (DEFAULT, %s, 0)",artist)
	
		except mdb.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0],e.args[1])

		finally:

			self.disconnect()

	def getStations(self):
		"""
		gets the list of stations from the database

		returns
		-------
		the list of stations in the database
		"""
		data = []
		output = []

		try:
			self.connect()

			self.cur.execute("select name from station")
	
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
	
		except _mysql.Error, e: 
			print "Error!"
			print "Error %d: %s" % (e.args[0], e.args[1])
	
		finally:

			self.disconnect()

			return output

	def getArtistsInDescendingOrder(self):
		"""
		gets the list of artists from the database in
		reverse order of popularity
		"""
		data = []
		output = []

		try:

			self.connect()
			self.cur.execute("select name from artist \
				order by popularity")

			data = self.cur.fetchall()

			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])

		except:

			print "Error!"

		finally:
			self.disconnect()

			return output

	def get100ArtistFromTop500(self):
		"""
		returns a list of 100 artists selected form the top 500
		"""
		data = []
		output = []

		try:
			self.connect()

			self.cur.execute("select name from artist \
				where popularity > 9\
				order by rand() \
				limit 100")
	
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
	
		except: 
			print "Error!"
	
		finally:

			self.disconnect()

			return output

	def getA2TCount(self):
		"""
		gets the list of A2T entries from the database in
		order of the number of artists which are linked to
		the tags

		returns
		-------
		list of tags in the database, in order of number of
		artists linking to one tag
		"""
		data = []
		output = []

		try:
			self.connect()

			self.cur.execute("SELECT tags.id, tags.name, COUNT( * ) AS cnt\
			FROM tags\
			INNER JOIN a2t ON a2t.tagid = tags.id\
			WHERE a2t.score >5\
			GROUP BY tags.id\
			ORDER BY cnt")
	
			data = self.cur.fetchall()
			#turn data from tuples into list of items
	
		except: 
			print "Error!"
	
		finally:

			self.disconnect()

			return data

	def getArtists(self):
		"""
		gets the list of artists from the database in
		order of popularity

		returns
		-------
		list of artists in the database, in order of popularity
		"""
		data = []
		output = []

		try:
			self.connect()

			self.cur.execute("select name from artist \
				order by popularity desc")
	
			data = self.cur.fetchall()
			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])		
	
		except: 
			print "Error!"
	
		finally:

			self.disconnect()

			return output

