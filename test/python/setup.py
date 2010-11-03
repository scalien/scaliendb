import scaliendb
import sys

client = scaliendb.Client(["127.0.0.1:7080"])
#client._set_trace(True)

quorum_id = client.create_quorum(["100", "101"])
print("Creating quorum(%s)..." % quorum_id)
if quorum_id == None:
	print("Error!")
	sys.exit(1)

database_id = client.create_database("message_board")
print("Creating database message_board(%s)..." % database_id)
if database_id == None:
	print("Error!")
	sys.exit(1)

table_id = client.create_table(int(database_id), int(quorum_id), "messages")
print("Creating table messages(%s)..." % table_id)
if table_id == None:
	print("Error!")
	sys.exit(1)

client.use_database("message_board")
client.use_table("messages")

print("Waiting for primary lease...")

client.set("index", "0")
print("Initializing messages:index to 0...")
