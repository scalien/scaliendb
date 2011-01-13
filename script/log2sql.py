import os
import sys
import struct
import signal
import string

FILTER = False

def write_command(data, context_id, shard_id):
	command = data[0]
	use_previous = data[1]
	nread = 2
	if use_previous == "F":
		context_id, shard_id = struct.unpack("<HQ", data[nread:nread+10])
		nread += 10
	if command == "s":
		# SET command
		keylen, = struct.unpack("<H", data[nread:nread+2])
		nread += 2
		key = data[nread:nread+keylen]
		nread += keylen
		vallen, = struct.unpack("<I", data[nread:nread+4])
		nread += 4
		val = data[nread:nread+vallen]
		nread += vallen
		# only write out data contexts
		if FILTER and context_id != 3:
			pass
		else:
			print("INSERT INTO kv VALUES (%d, %d, '%s', '%s');" % (context_id, shard_id, key, val))
	elif command == "d":
		# DELETE command
		keylen, = struct.unpack("<H", data[nread:nread+2])
		nread += 2
		key = data[nread:nread+keylen]
		nread += keylen
		# only write out data contexts
		if FILTER and context_id != 3:
			pass
		else:
			print("DELETE FROM kv WHERE context_id=%d AND shard_id=%d AND k='%s';" % (context_id, shard_id, key))
	return nread, context_id, shard_id

def write_sql_commands(data):
	context_id = 0
	shard_id = 0
	while len(data) > 0:
		nread, context_id, shard_id = write_command(data, context_id, shard_id)
		data = data[nread:]

if __name__ == "__main__":
	filename = sys.argv[1]
	archive_name = filename
	if len(sys.argv) > 2:
		# move the file to the archive dir
		archive_dir = sys.argv[2]
		archive_name = os.path.join(archive_dir, os.path.basename(filename))
		#print("Renaming %s to %s" % (filename, archive_name))
		os.rename(filename, archive_name)
		# create output sql file
		sql_filename = archive_name.replace("log.", "sql.")
		sql_file = open(sql_filename, "w+")
		sys.stdout = sql_file
	f = open(archive_name, "r")
	data = f.read(8)
	log_segment_id,  = struct.unpack("<Q", data)
                    
	while True:     
		data = f.read(8)
		if data == "":
			break
		size, crc = struct.unpack("<II", data)
		data = f.read(size - 8)
		write_sql_commands(data)
