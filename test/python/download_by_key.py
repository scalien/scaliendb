import random
import sys
import scaliendb
import time

CONTROLLERS=["localhost:7080"]
#CONTROLLERS=["192.168.137.51:7080"]

key = ""
filename = None
if len(sys.argv) > 1:
	key = sys.argv[1]
if len(sys.argv) > 2:
	filename = sys.argv[2]
	
client = scaliendb.Client(CONTROLLERS)
#client._set_trace(True)

if False:
	quorum_id = client.create_quorum(["100"])
	database_id = client.create_database("testdb")
	client.create_table(database_id, quorum_id, "testtable")
                        
client.use_database("testdb")
client.use_table("testtable")
data = client.get(key)
f = open(filename, "wb")
f.write(data)
