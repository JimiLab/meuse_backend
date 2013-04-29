"""
A wrapper for the sqlite database
"""

import sqlite3

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""

	database = "database/meuse.db"

	def addArtistsToStation(self, artistsAndStations):
		"""
		populates station and artistToStation database. score
		for each artist is always 0

		artistToStation: a dictionary where artist is mapped
		to a list of stations
		"""
		for artist in artistToStation:

			stations = artistsAndStations[artist]

			for station in stations:			
				#put station in to the list
				self.addStation(station)

				#get station id
				stationID = self.getStationID(station)

				#get artist id
				artistID = self.getArtistID(artist)

				#put station into artistToStation, score is 0
				self.addArtistToStation(artistID, stationID, 0)

	def getStationID(self, station):
		"""
		gets the id for the station passed in
		station: name of the station
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select id from station where name=(?)", station)

			data = cursor.fetchall()

			#turn data from tuples into list of items
			stationID = data[0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return stationID

	def getArtistID(self, station):
		"""
		gets the id for the artist passed in
		station: name of the artist
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select id from artist where name=(?)", station)

			data = cursor.fetchall()

			#turn data from tuples into list of items
			artistID = data[0]

		except sqlite3.Error, e:

			print "Error!"

			print "Error %s:" % e.args[0]

		finally:

			if con:

				con.close()

			return artistID

	def addArtistToStation(self, artistID, stationID, score):
		"""
		adds a station into the artistToStation table
		artistID: id of the artist
		stationID: id of the 
		score: score for the relationship
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			cursor.execute("insert into A2S(artistID, stationID, score) values (?,?,?)", artistID, stationID)

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

			cursor.execute("insert on conflict ignore \
				into artist(name) \
				values (?)", station)

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
		gets the list of artists from the database
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select name from artist")

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
