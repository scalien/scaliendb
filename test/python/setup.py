import scaliendb
import sys

client = scaliendb.Client(["127.0.0.1:7080"])
# scaliendb.set_trace(True)

quorum = client.create_quorum_cond(["100"], "test quorum")

db = client.create_database_cond("test db")
table = db.create_table_cond("test table", quorum)
table.set("foo", "bar")

print(client.get_quorums())
client.rename_quorum(quorum, "yet another quorum")
print(client.get_quorums())
