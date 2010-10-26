import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])

quorum_id = client.create_quorum(["100, 101"])
print("Creating quorum(%s)..." % quorum_id)

database_id = client.create_database("message_board")
print("Creating database message_board(%s)..." % database_id)

table_id = client.create_table(int(database_id), int(quorum_id), "messages")
print("Creating table messages(%s)..." % table_id)

client.use_database("message_board")
client.use_table("messages")

print("Waiting for primary lease...")

client.set("index", "0")
print("Initializing messages:index to 0...")
