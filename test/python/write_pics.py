import scaliendb
from scaliendb_load import *

client = scaliendb.Client(["localhost:7080"])
#client = scaliendb.Client(["192.168.137.51:7080"])
#client._set_trace(True)
client.use_database("bulk")
client.use_table("pics")
lr = scaliendb_loader(client, print_stats=True)
#f = open('data/pics.txt','r') 
f = open('pics.txt','r') 
images = {}
dataset = 500*1000*1000
size = 0
print("Reading images from disk...")
for key in f:
	key = key.rstrip()
	i = open(key, 'r')
	value = i.read()
	images[key] = value
	size += len(value)
	if size > dataset:
		break
print("Dataset size: %d" % (size))
print("Done.")
lr.begin()
for key, value in images.iteritems():
# for key in f:
# 	key = key.rstrip()
# 	i = open(key, 'r')
# 	value = i.read()
	lr.set(key, value)
lr.submit()
print("Done.")
f.close()