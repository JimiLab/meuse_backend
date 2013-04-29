"""
A wrapper for the sqlite database
"""

import sqlite3

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""

	database = "database/meuse.db"

	def addArtistPopularity(self, artists):
		"""
		adds a list of artists into the database. 
		artists is a list of dictionaries containing the 
		following
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			for artist in artists:

				popularity = artists['popularity']
				name = artists['name']

				cursor.execute("update artist \
					set popularity = (?) \
					where name = (?)", popularity, name)

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
