#!/usr/bin/env python
"""
Usage: dbtool.py command [options]
	
Command can be any of:

	analyze-recovery | ar	Prints the content of a recovery file
	analyze-datafile | ad	Prints the content of a data file

See 'dbtool.py help COMMAND' for more information about a specific command.
"""

import os
import sys
import struct

def print_recovery_op(i, op, size, data):
	if op == 0:
		print("%4d. op = DONE" % (i))
	elif op == 1:
		page_header = data[:16]
		page_size, file_index, offset, key_count = struct.unpack("<IIII", page_header)
		print("%4d. op = PAGE, size = %d, fileIndex = %d, offset = %d, keys = %d" % (i, size, file_index, offset, key_count))
	elif op == 2:
		file_index, = struct.unpack("<I", data)
		print("%4d. op = FILE, fileIndex = %d" % (i, file_index))
	elif op == 3:
		prev_index, = struct.unpack("<I", data)
		print("%4d. op = COMMIT, prevCommitStorageFileIndex = %d" % (i, prev_index))
	else:
		print("%4d. op = %d, size = %d" % (i, op, size))

def analyze_recovery(filename, *args):
	"""
	Usage: dbtool.py analyze-recovery <recovery-file>
	"""
	f = open(filename, "r")
	if os.path.getsize(filename) == 0:
		print("Empty recovery file")
		return
	i = 1
	while True:
		op_head = f.read(8)
		if op_head == "":
			break
		op, size = struct.unpack("<II", op_head)
		data = f.read(size)
		print_recovery_op(i, op, size, data)
		i += 1

def print_datafile_header(header):
	type, version, crc = struct.unpack("<32sII", header[:40])
	minor = version % 256
	major = version >> 8
	print("type = %s, version = %d.%d, crc = 0x%08x" % (type, major, minor, crc))

def print_indexpage_header(indexpage_header):
	indexpage_size, datapage_size, num_datapage_slots = struct.unpack("<III", indexpage_header)
	print("indexPageSize = %d, dataPageSize = %d, numDataPageSlots = %d" % (indexpage_size, datapage_size, num_datapage_slots))
	return indexpage_size, datapage_size, num_datapage_slots

def print_page(page_header, data, verbose=0):
	page_size, file_index, offset, key_count = struct.unpack("<IIII", page_header)
	print("pageSize = %6d, fileIndex = %d, offset = %8d, keys = %d" % (page_size, file_index, offset, key_count))
	if page_size == 64*1024:
		if verbose > 1:
			rest = data
			count = 0
			while count < key_count:
				keylen, = struct.unpack("<I", rest[:4])
				rest = rest[4:]
				key, = struct.unpack("<" + str(keylen) + "s", rest[:keylen])
				rest = rest[keylen:]
				vallen, = struct.unpack("<I", rest[:4])
				rest = rest[4:]
				val, = struct.unpack("<" + str(vallen) + "s", rest[:vallen])
				rest = rest[vallen:]
				print("\tkey = %s, val = %s" % (key, val))
				count += 1
	if page_size == 256*1024:
		if verbose > 0:
			rest = data
			count = 0
			while count < key_count:
				keylen, = struct.unpack("<I", rest[:4])
				rest = rest[4:]
				key, = struct.unpack("<" + str(keylen) + "s", rest[:keylen])
				rest = rest[keylen:]
				index, = struct.unpack("<I", rest[:4])
				rest = rest[4:]
				print("\tindex = %3d, key = %s" % (index, key))
				count += 1

def offset_to_index(offset, indexpage_size, datapage_size):
	return (offset - 4096 - indexpage_size) / datapage_size

def analyze_datafile(filename, args):
	"""
	Usage: dbtool.py analyze-datafile <data-file>
	"""
	f = open(filename, "r")
	i = 1
	datafile_header = f.read(4096 - 12)
	print_datafile_header(datafile_header)
	indexpage_header = f.read(12)
	indexpage_size, datapage_size, num_datapage_slots = print_indexpage_header(indexpage_header)
	verbose = 0
	if "-v" in args:
		verbose = 1
	if "-vv" in args:
		verbose = 2
	offset = 4096
	while True:
		page_header = f.read(16)
		if page_header == "":
			return
		if len(page_header) < 16:
			print("Error")
			return
		offset += len(page_header)
		page_size, = struct.unpack("<I", page_header[:4])
		if page_size < 65536:
			index = offset_to_index(offset, indexpage_size, datapage_size)
			print("Error pageSize = %d at offset = %d (%4d)" % (page_size, offset, index))
			return
		page_data = f.read(page_size - 16)
		print_page(page_header, page_data, verbose=verbose)
		offset += len(page_data)

def analyze_shardindex(filename, args):
	"""
	Usage: dbtool.py analyze-shardindex <index-file>
	"""
	f = open(filename, "r")
	datafile_header = f.read(4096 - 12)
	print_datafile_header(datafile_header)
	index_page_header = f.read(4)
	file_count, = struct.unpack("<I", index_page_header)
	while True:
		index_header = f.read(8)
		if index_header == "":
			return
		index, keylen = struct.unpack("<II", index_header)
		keydata = f.read(keylen)
		data, = struct.unpack("<" + str(keylen) + "s", keydata)
		print("index = %4d, key = %s" % (index, data))

def analyze_file(filename, args):
	f = open(filename, "r")
	header = f.read(4096 - 12)
	if os.path.getsize(filename) == 0:
		print("Empty file")
		return
	if header[:9].find("ScalienDB") == -1:
		if filename.find("recovery") != -1:
			print("Assuming recovery file...")
			analyze_recovery(filename, args)
		else:
			print("Unknown file")
		return
	type, version, crc = struct.unpack("<32sII", header[:40])
	if type.find("ScalienDB data file") == 0:
		analyze_datafile(filename, args)
	elif type.find("ScalienDB shard index") == 0:
		analyze_shardindex(filename, args)
	else:
		print("Unknown file")

def help(command):
	g = globals()
	command = command.replace("-", "_")
	if g.has_key(command):
	   	print(g[command].__doc__)
   	else:
		print("No such command!")

if __name__ == "__main__":
	if len(sys.argv) < 3:
		print __doc__
		sys.exit(1)
	command = sys.argv[1]
	function = help
	if command == "analyze-recovery" or command == "ar":
		function = analyze_recovery
	elif command == "analyze-datafile" or command == "ad":
		function = analyze_datafile
	elif command == "analyze-shardindex" or command == "as":
		function = analyze_shardindex
	elif command == "analyze-file" or command == "af":
		function = analyze_file
	function(sys.argv[2], sys.argv[3:])