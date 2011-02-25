import struct
import scaliendb

client = scaliendb.Client(["127.0.0.1:7080"])
client._set_trace(True);

client.use_database("testdb")
client.use_table("testtable")

value = "\x01\x00"
print(len(value))
client.get(value)
