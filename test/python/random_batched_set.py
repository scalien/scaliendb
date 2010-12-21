import random
import sys
import scaliendb

#CONTROLLERS=["localhost:7080"]
CONTROLLERS=["192.168.137.51:7080"]

limit = 0
if len(sys.argv) > 1:
	limit = int(sys.argv[1])
client = scaliendb.Client(CONTROLLERS)
client._set_trace(True)
client.use_database("testdb")
client.use_table("testtable")
sent = 0
value = "%1000s" % " "
while limit == 0 or sent < limit:
	client.begin()
	for x in xrange(1000):
		client.set(str(random.randint(1, 1000000000)), value)
		sent += len(value)
	client.submit()
