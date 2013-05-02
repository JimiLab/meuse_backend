"""
A wrapper for the sqlite database
"""

import sqlite3

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""
	database = "database/meuse.db"

	def getStationSetForArtist(self, artist):
		"""
		gets a dataset for clustering for a given
		artist. The dataset is as follows:

		"labels" = list of stations for each set
		"data" = list of artist for each station
		"""
		dataset = {"labels" : [], "data" : []}
		artists = ""
		stations = self.getStationsForArtist(artist)

		for station in stations:

			#get data from dataset and convert into a long string
			artistsForStation = self.getArtistsForStation(station)

			for artist in artistsForStation:

				artists = artists + " " + artist

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
				and station.id = a2s.stationID", [artist])

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
		updates the popularity of a station

		parameters
		----------
		station: name of the station
		newPopularity: popularity of the station	
		"""

		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("update station set popularity=(?) \
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

	def updateArtistToA2S(self, artistID, stationID, score):
		"""
		updates a row of the artistToStation table with a new
		score.
		artistID: id of the artist
		stationID: id of the 
		score: score for the relationship
		"""
		print "score " + str(score) + " artistID " + \
		str(artistID) + " station ID " + str(stationID)

		con = sqlite3.connect(self.database)

		try:

			cursor = con.cursor()

			cursor.execute("update A2S set score=(?) \
			where artistID = (?) and stationID = (?)", (score, artistID, stationID))

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

	def addStation(self, station, popularity):
		"""
		adds a station into the database. Turns into an update if it exists

		parameters
		----------
		station: the name of the station
		popularity: the listen count of the station
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			cursor.execute("insert \
				into station (name, popularity) \
				values (?, ?)", (station, popularity))

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

	def getStations(self):
		"""
		gets the list of stations from the database
		"""
		con = sqlite3.connect(self.database)
		data = []
		output = []

		try:

			cursor = con.cursor()

			cursor.execute("select name from station")

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
