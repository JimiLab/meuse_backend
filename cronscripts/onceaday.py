"""
This cron script is automatically run once a day

following activities are done:
------------------------------
* checks which stations play which artists
"""

import sys
sys.path.insert(0, '/Users/meuse/Meuse/env/meuse/meuse/meuse')
from downloader import Downloader

downloader = Downloader()

#downloads the stations
downloader.download_stations()
