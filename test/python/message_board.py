import time
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])
#client._set_trace(True)

client.use_database("message_board")
client.use_table("messages")

print("ScalienDB Messageboard sample 0.10")

highest_seen = 0
while True:
	index = client.get("index")
	if index is not None:
		index = int(index)
		for i in xrange(highest_seen, index):
			message = client.get("%d" % i)
			if message is not None:
				print("%d. %s" % (i, message))
			else:
				print("None")
		highest_seen = index
	time.sleep(0.5)
