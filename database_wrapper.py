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
			self.con = mdb.connect("localhost", self.username, self.password, self.database)
			self.cur = self.con.cursor()

		except _mysql.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
	
	def disconnect(self):
		"""	
		disconnects from database
		"""	
		if (self.con):
			self.con.close()

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

	def getStationsForArtist(self, artist):
		"""
		gets the list of stations which have
		played the given artist

		artist: the name of the artist
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select station.name \
				from station, artist, a2s \
				where artist.name=(?) \
				and artist.id = a2s.artistID \
				and station.id = a2s.stationID \
				order by station.popularity", [artist])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return output

	def getArtistsForStation(self, station):
		"""
		gets the list of artists who have been 
		played on the given station

		station: name of the station
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select artist.name \
				from station, artist, a2s \
				where station.name=(?) \
				and artist.id = a2s.artistID \
				and station.id = a2s.stationID", [station])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			for item in data:
				output.append(item[0])

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

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
				tagID = self.getTagID(tag)

				#if tag does not exist, add to tags
				if (tagID == 0):
					self.addTag(tag)
					tagID = self.getTagID(tag)

				#if a2t entry does not exist, add it
				if (self.checkIfA2TExists(artistID, tagID) == 0):
					self.addA2T(artist, tag)
					a2tID = self.checkIfA2TExists(artistID, tagID)

	def addA2T(self, artist, tag):
		"""
		adds an entry to the a2t table
		"""		
		con = sqlite3.connect(self.database)
		artist_key = 1

		artistID = self.getArtistID(artist)
		tagID = self.getTagID(tag)

		try:

			cursor = con.cursor()

			cursor.execute("insert \
				into a2t (artistID, tagID) \
				values (?, ?)", (artistID, tagID))

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def checkIfStationExists(self, station):
		"""
		checks if a station exists in the database
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from station where \
				name=(?)", [station])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return output

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
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from artist \
				where name=(?)", [artist])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

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
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from a2a where \
				artist1ID=(?) and artist2ID=(?)", (artistID, simartistID))

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return output

	def checkIfA2TExists(self, artistID, tagID):
		"""
		checks if an a2t entry exists
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from a2t where \
				tagID=(?) and artistID=(?)", (tagID, artistID))

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return output


	def addTag(self, tagName):
		"""
		inserts a tag into the database
		"""
		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("insert into tags (name) values (?)", [tagName])

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def getTagID(self, tagName):
		"""
		returns the ID for a tag, or 0 if it does not
		exist
		"""
		#get artistID and stationID
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from tags where name=(?)", [tagName])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return output


	def getArtistToStationScore(self, artist, station):
		"""
		gets the score of an artist in the A2S table
		"""
		#get artistID and stationID
		con = sqlite3.connect(self.database)
		data = []
		output = 0

		artistID = self.getArtistID(artist)
		stationID = self.getStationID(station)

		try:

			cursor = con.cursor()

			cursor.execute("select score from a2s where\
				artistID = (?) and stationID = (?)", (artistID, stationID))

			data = cursor.fetchall()

			#turn data from tuples into list of items
			output = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

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
				
				#check if station exists
				if (self.checkIfStationExists(station)==0):
					#put station in to the list
					self.addStation(station, popularity)

				#otherwise, update the station popularity
				else:
					self.updateStationPopularity(station, popularity)

				#get station id
				stationID = self.getStationID(station)

				#get artist id
				artistID = self.getArtistID(artist)

				#get current station score
				score = self.getArtistToStationScore(artist, station)

				if score == 0:
					#create new row
					self.addArtistToA2S(artistID, stationID, 1)

				else:
					#update row
					self.updateArtistToA2S(artistID, stationID, score + 1)

	def getStationID(self, station):
		"""
		gets the id for the station passed in
		station: name of the station
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []
		stationID = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from station where name=(?)", [station])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			stationID = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return stationID

	def getArtistID(self, artist):
		"""
		gets the id for the artist passed in
		station: name of the artist
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []
		artistID = 0

		try:

			cursor = con.cursor()

			cursor.execute("select id from artist where name=(?)", [artist])

			data = cursor.fetchall()

			#turn data from tuples into list of items
			artistID = data[0][0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return artistID

	def updateStationPopularity(self, station, newPopularity):
		"""
		updates the popularity of a station as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		station: name of the station
		newPopularity: popularity of the station	
		"""



		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("update station set \
				popularity=((station.popularity + (?))/2) \
			where name = (?)", 
			(newPopularity, station))

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def updateArtistPopularity(self, artist, newPopularity):
		"""
		updates the popularity of an artist as follows

		popularity = (currentPop + oldPop) / 2 

		parameters
		----------
		artist: name of the artist
		newPopularity: popularity of the station	
		"""

		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("update artist set \
				popularity=((artist.popularity + (?))/2) \
			where name = (?)", 
			(newPopularity, artist))

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

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

	def addStation(self, station, popularity):
		"""
		adds a station into the database. Turns into an update if it exists

		parameters
		----------
		station: the name of the station
		popularity: the listen count of the station
		"""
		try:
			self.connect()

			self.cur.execute("insert into station(id, name, popularity) \
			values (DEFAULT, %s, %s)",(station, popularity))

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

