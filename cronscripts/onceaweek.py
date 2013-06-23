"""
This cron script is automatically run once a week

following activities are done:
------------------------------
* checks for artist tags
* decays the a2s property by 0.95
* updates the artist popularity 
* checks artist similarity
"""

import sys
sys.path.insert(0, '/Users/meuse/Meuse/env/meuse/meuse/meuse')
from downloader import Downloader

downloader = Downloader()

#get taags
downloader.download_tags()

#decay artist popularity
downloader.decay_a2s()

#get popularity
downloader.update_artist_popularity()

#get similar artists
downloader.download_similar_artists()


