"""
interface for pyramid. 

searches the database for the given artist and returns a set of 3 stations. Each station has the following information:
1 representative artist
3 representative tags
"""

from database_wrapper import DatabaseWrapper
from cluster_module import ClusterModule


