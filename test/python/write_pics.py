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

# value = open("/Users/mtrencseni/Pictures/iPhoto Library/Data/2009/ELTE/IMG_4268.jpg", 'r').read()
# lr = scaliendb_loader(client, granularity=100*1000, print_stats=True)
# lr.begin()
# for i in xrange(0, 1000*1000):
	# client.set(hash(str(i)), value)
	# client.set(str(i), value)
	# lr.set(hash(str(i)), value)
#     lr.set(str(i), str(i))
# lr.submit()

num = 1*1000

for i in xrange(0, num):
    client.set(str(i), str(i))
client.submit()

# for i in xrange(0, num/2):
#     client.set(str(i), str(i*i))
# for i in xrange(0, num):
#     print(client.get(str(i)))
# client.submit()
# 
# print("")
# 
# for i in xrange(0, num):
#     client.set(str(i), str(i*i*i))
#     print(client.get(str(i)))
# client.submit()
# 
# print("")
# print(client.get("foo"))
# print("")
# 
# for i in xrange(0, num):
#     client.set(str(i), str(i*i*i*i))
#     client.delete(str(i))
#     print(client.get(str(i)))
# client.submit()


# i = 0
# try:
# 	for i in xrange(0, 100000):
# 		client.set(hash(str(i)), value)
# 		print(i)
# except KeyboardInterrupt:
# 	print("\n Sent items: %d" % i)	
