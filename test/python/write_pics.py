import scaliendb
from scaliendb_load import *

client = scaliendb.Client(["localhost:7080"])
#client._set_trace(True)
client.use_database("bulk")
client.use_table("pics")
lr = scaliendb_loader(client, print_stats=True)
f = open('data/pics.txt','r') 
lr.begin()
i = None
for key in f:
	key = key.rstrip()
	if i is None:
		i = open(key, 'r')
		value = i.read()
	lr.set(key, value)
	#print(key)
lr.submit()
print("Done.")
f.close()