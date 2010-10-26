import pickle
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])

client.use_database("message_board")
client.use_table("messages")

message = {}
message["author"] = raw_input("author: ")
message["body"] = raw_input("body: ")
smessage = pickle.dumps(message)

index = int(client.add("index", 1))

client.set("%d" % (index - 1), smessage)

print("Message saved.")
