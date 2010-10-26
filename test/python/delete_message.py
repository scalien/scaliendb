import sys
import pickle
import scaliendb

if len(sys.argv) < 1:
	print("Specify index of message to delete")

client = scaliendb.Client(["127.0.0.1:7080"])

client.use_database("message_board")
client.use_table("messages")

client.delete("%d" % int(sys.argv[len(sys.argv)-1]))

print("Message deleted.")
