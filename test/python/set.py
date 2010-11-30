import pickle
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])

client._set_trace(True);

client.use_database("test_db")
client.use_table("test_table")

client.set("key", "value")
