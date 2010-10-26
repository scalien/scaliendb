import scaliendb

client = scaliendb.Client(["localhost:7080"])
client._set_trace(True)
client.use_database("testdb")
client.use_table("testtable")
for i in xrange(10):
	client.set("key%s" % i, "value%s" % i)

for i in xrange(10):
	value = client.get("key%s" % i)
	print(value)


client.set("user_id", "0")
client.add("user_id", 1)
client.add("user_id", 1)
client.add("user_id", 1)
client.add("user_id", 1)
print(client.get("user_id"))

