import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])
client._set_trace(True)

print(client.create_quorum(["100"]))

print(client.create_database("message_board"))
print(client.create_table(1, 1, "messages"))

client.use_database("message_board")
client.use_table("messages")

client.set("index", "0")
