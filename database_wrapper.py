"""
A wrapper for the sqlite database
"""

import sqlite3

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""
	database = "database/meuse.db"

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

		artistToStation: a dictionary where artist is mapped
		to a list of stations
		"""
		for artist in artistsAndStations:

			stations = artistsAndStations[artist]

			for station in stations:	
				
				#check if station exists
				if (self.checkIfStationExists(station)==0):
					#put station in to the list
					self.addStation(station)

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
					self.updateArtistToA2S(artist, station, score + 1)

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

	def updateArtistToA2S(self, artistID, stationID, score):
		"""
		updates a row of the artistToStation table with a new
		score.
		artistID: id of the artist
		stationID: id of the 
		score: score for the relationship
		"""

		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("update A2S set score=(?) \
			where artistID = (?) and stationID = (?)", score, artistID, stationID)

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def addArtistToA2S(self, artistID, stationID, score):
		"""
		adds a station into the artistToStation table.
		artistID: id of the artist
		stationID: id of the 
		score: score
		"""

		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("insert into A2S(artistID, stationID, score)\
			 values (?,?,?)", (artistID, stationID, score))

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def addStation(self, station):
		"""
		adds a station into the database. Turns into an update if it exists

		station: the name of the station
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			cursor.execute("insert \
				into station(name) \
				values (?)", [station])

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def addArtistPopularity(self, artists):
		"""
		adds the popularity of each artist into the 
		database.

		artists: a dictionary where artists is mapped
		to popularity.
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			for artist in artists:

				popularity = artists[artist]

				cursor.execute("update artist \
					set popularity = (?) \
					where name = (?)", (popularity, artist))

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()


	def addArtists(self, artists):
		"""
		adds a list of artists into the database
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			for artist in artists:

				cursor.execute("insert into artist(name) values (?)", [artist])

		except sqlite3.Error, e:

			if con: con.rollback()

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.commit()

				con.close()

	def getArtists(self):
		"""
		gets the list of artists from the database in
		order of popularity
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select name from artist \
				order by popularity desc")

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

	def executeStatement(self, statement):
		"""
		executes the passed in SQL statement and returns the 
		result
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute(statement)

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
