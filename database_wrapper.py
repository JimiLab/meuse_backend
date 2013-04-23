"""
A wrapper for the sqlite database
"""

import sqlite3

class DatabaseWrapper:
	"""
	Wrapper for the sqlite database
	"""

	database = "database/meuse.db"

	def addArtists(self, artist, logplays):
		"""
		adds an artist and the log of the number of plays
		into the database
		"""
		con = sqlite3.connect(self.database)
		artist_key = 1

		try:

			cursor = con.cursor()

			for artist in artists:

				cursor.execute("insert into artists(name, artist) values (?, ?)", ([artist], logplays))

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

			cursor.execute("select name from artists")

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

	def 
