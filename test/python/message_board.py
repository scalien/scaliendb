import time
import pickle
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])

client.use_database("message_board")
client.use_table("messages")

def print_message(i, message):
	print("")
	print("%d. %s: %s" % (i, message["author"], message["body"]))
	print("")
	print("-------------")

print("ScalienDB Messageboard sample 0.10")

highest_seen = 0
while True:
	index = client.get("index")
	if index is not None:
		index = int(index)
		for i in xrange(highest_seen, index):
			smessage = client.get("%d" % i)
			if smessage is not None:
				message = pickle.loads(smessage)
				print_message(i, message)
		highest_seen = index
	time.sleep(0.5)
