import scaliendb
import time

client = scaliendb.Client(["127.0.0.1:7080"])

client.use_database("test")
client.use_table("test")
client.truncate_table("test")

starttime = time.time()
for i in xrange(1345):
    client.set(i, i*i)
client.submit()
endtime = time.time()
elapsed = endtime - starttime
print(elapsed)
