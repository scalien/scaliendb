import random
import sys
import scaliendb
import time

CONTROLLERS=["localhost:7080"]
#CONTROLLERS=["192.168.137.51:7080"]

start_key = ""
count = 0
offset = 0
if len(sys.argv) > 1:
	start_key = sys.argv[1]
if len(sys.argv) > 2:
	count = int(sys.argv[2])
if len(sys.argv) > 3:
	offset = int(sys.argv[3])
	
client = scaliendb.Client(CONTROLLERS)
#client._set_trace(True)

if False:
	quorum_id = client.create_quorum(["100"])
	database_id = client.create_database("testdb")
	client.create_table(database_id, quorum_id, "testtable")
                        
client.use_database("testdb")
client.use_table("testtable")
num = client.count(start_key, count, offset)
print(num)