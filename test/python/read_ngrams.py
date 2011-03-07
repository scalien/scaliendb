import random
import sys
import scaliendb
import time
import string
import csv
from scaliendb_iter import *
	
client = scaliendb.Client(["localhost:7080"])
#client._set_trace(True)
client.use_database("bulk")
client.use_table("1-grams")
lr = scaliendb_iter_keyvalues(client, count=1000)
for k, v in lr:
	#print("%s => %s" % (k, v))
	