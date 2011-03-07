import csv
import scaliendb
from scaliendb_load import *

client = scaliendb.Client(["localhost:7080"])
#client._set_trace(True)
client.use_database("bulk")
client.use_table("1-grams")
lr = scaliendb_loader(client, print_stats=True)
reader = csv.reader(open('data/googlebooks-eng-all-1gram-20090715-0.csv', 'rb'), delimiter='\t', quotechar='|')
lr.begin()
for row in reader:
	key = "%s/%s" % (row[0], row[1])
	value = "%s\t%s\t%s\t%s\t%s" % (row[0], row[1], row[2], row[3], row[4])
	lr.set(key, value)
lr.submit()
print("Done.")