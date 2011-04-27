import random
import sys
import scaliendb
import time
import md5
import string
import base64
import os

CONTROLLERS=["localhost:7080"]
#CONTROLLERS=["192.168.137.51:7080"]

def human_bytes(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1000.0:
			return "%3.1f%s" % (num, x)
		num /= 1000.0


def random_string(leng):
	nbits = leng * 6 + 1
	bits = random.getrandbits(nbits)
	uc = u"%0x" % bits
	newlen = int(len(uc) / 2) * 2 # we have to make the string an even length
	ba = bytearray.fromhex(uc[:newlen])
	return base64.urlsafe_b64encode(str(ba))[:leng]

limit = 0
start = 0
if len(sys.argv) > 1:
	limit = int(sys.argv[1])
	if len(sys.argv) > 2:
		start = int(sys.argv[2])
client = scaliendb.Client(CONTROLLERS)
#client._set_trace(True)

if False:
	quorum_id = client.create_quorum(["100"])
	database_id = client.create_database("testdb")
	client.create_table(database_id, quorum_id, "testtable")

if False:
	database_id = client.get_database_id("testdb")
	table_id = client.get_table_id(database_id, "testtable")
	client.truncate_table_by_id(table_id)
                        
client.use_database("testdb")
client.use_table("testtable")
sent = 0
value = "%100s" % " "
i = start
batch = 100
if limit != 0 and limit < batch:
	batch = limit
random_value = random_string(1000000)
starttime = time.time()
while limit == 0 or i < limit:
        keys = []
        values = []
        value_len = 50000
        for x in xrange(batch):
                keys.append(md5.new(str(x + i)).hexdigest())
                #values.append(''.join(random.choice(string.ascii_uppercase + string.digits) for v in range(random.randint(1, value_len))))
		#values.append(random_string(random.randint(1, value_len)))
		#values.append(os.urandom(random.randint(1, value_len)))
                #values.append(value)
		#values.append(random_string(100000))
		#values.append(random_value[random.randint(1, len(random_value)):])
		offset = random.randint(0, len(random_value) - value_len - 1)
		values.append(random_value[offset:offset+value_len])
                #print("Generating random: %d/%d\r" % (x, batch))
	client.begin()
	round_sent = 0
	for x in xrange(batch):
		#client.set(str(random.randint(1, 1000000000)), value)
		#key = md5.new(str(x + i)).hexdigest()
		#print(x+i, key)
		client.set(keys[x], values[x])
		#client.set(keys[x], keys[x])
		sent += len(values[x])
		round_sent += len(values[x])
	ret = client.submit()
	if ret != 0:
		print("Submit failed!")
		break
	endtime = time.time()
	i += batch
	fmt = "%H:%M:%S"
	endtimestamp = time.strftime(fmt, time.gmtime())
	elapsed = (endtime - starttime)
	print("%s: Sent bytes: %s (%s/s), num: %i, rps = %.0f" % (endtimestamp, human_bytes(sent), human_bytes(sent / elapsed) , i, (i/((endtime - starttime) * 1000.0) * 1000.0)))
