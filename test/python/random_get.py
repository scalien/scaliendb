import random
import sys
import scaliendb
import time
import md5

CONTROLLERS=["localhost:7080"]
#CONTROLLERS=["192.168.137.51:7080"]

def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0

limit = 0
start = 0
if len(sys.argv) > 1:
	limit = int(sys.argv[1])
	if len(sys.argv) > 2:
		start = int(sys.argv[2])
client = scaliendb.Client(CONTROLLERS)
#client._set_trace(True)

if False:
	quorum_id = client.create_quorum(["100"])
	database_id = client.create_database("testdb")
	client.create_table(database_id, quorum_id, "testtable")
                        
client.use_database("testdb")
client.use_table("testtable")
sent = 0
value = "%100s" % " "
i = start
batch = 10000
starttime = time.time()
rnd = random.randint(1, 1000000000)
print(rnd)
ret = client.get(md5.new(str(rnd)).hexdigest())
endtime = time.time()
print("Elapsed: %f" % (endtime - starttime))