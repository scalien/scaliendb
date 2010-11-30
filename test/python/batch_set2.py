import pickle
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])

#client._set_trace(True);

client.use_database("test_db")
client.use_table("test_table")

client.begin()
for i in xrange(100*1000):
	client.set("key%s" % i, "value%s" % i)
client.submit()
print("done")
