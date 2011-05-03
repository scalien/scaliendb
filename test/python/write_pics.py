import scaliendb
from scaliendb_load import *
import md5
import os, sys, time

def hash(key):
	m = md5.new()
	m.update(key)
	return m.hexdigest()


client = scaliendb.Client(["localhost:7080"])
# client = scaliendb.Client(["192.168.137.51:7080"])
# scaliendb.set_trace(True)
client.use_database("bulk")
client.use_table("pics")
# client.use_table("test")

# for i in xrange(0, 10	*1000):
# 	client.set("0", "0")

value = open("/Users/mtrencseni/Pictures/iPhoto Library/Data/2009/ELTE/IMG_4268.jpg", 'r').read()
lr = scaliendb_loader(client, print_stats=True)
lr.begin()
for i in xrange(0, 10000000):
	# client.set(hash(str(i)), value)
	# client.set(str(i), value)
	# lr.set(hash(str(i)), value)
	lr.set(str(i), value)
lr.submit()

# i = 0
# try:
# 	for i in xrange(0, 100000):
# 		client.set(hash(str(i)), value)
# 		print(i)
# except KeyboardInterrupt:
# 	print("\n Sent items: %d" % i)	
