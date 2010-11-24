#!/usr/bin/env python
"""
Usage: dbtool.py command [options]
	
Command can be any of:

	analyze-recovery | ar	Prints the content of a recovery file
	analyze-datafile | ad	Prints the content of a data file

See 'dbtool.py help COMMAND' for more information about a specific command.
"""

import sys
import struct

def print_recovery_op(i, op, size, data):
	if op == 0:
		print("%d. op = DONE" % (i))
	elif op == 1:
		page_header = data[:16]
		page_size, file_index, offset, key_count = struct.unpack("<IIII", page_header)
		print("%d. op = PAGE, size = %d, fileIndex = %d, offset = %d, keys = %d" % (i, size, file_index, offset, key_count))
	elif op == 2:
		file_index, = struct.unpack("<I", data)
		print("%d. op = FILE, fileIndex = %d" % (i, file_index))
	elif op == 3:
		prev_index, = struct.unpack("<I", data)
		print("%d. op = COMMIT, prevCommitStorageFileIndex = %d" % (i, prev_index))
	else:
		print("%d. op = %d, size = %d" % (i, op, size))

def analyze_recovery(filename):
	"""
	Usage: dbtool.py analyze-recovery <recovery-file>
	"""
	f = open(filename, "r")
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
	major = version % 256
	minor = version & 0xFF
	print("type = %s, version = %d.%d, crc = %d" % (type, major, minor, crc))

def print_indexpage_header(indexpage_header):
	indexpage_size, datapage_size, num_datapage_slots = struct.unpack("<III", indexpage_header)
	print("indexPageSize = %d, dataPageSize = %d, numDataPageSlots = %d" % (indexpage_size, datapage_size, num_datapage_slots))

def print_page(page_header, data):
	page_size, file_index, offset, key_count = struct.unpack("<IIII", page_header)
	print("pageSize = %d, fileIndex = %d, offset = %d, keys = %d" % (page_size, file_index, offset, key_count))

def analyze_datafile(filename):
	"""
	Usage: dbtool.py analyze-datafile <data-file>
	"""
	f = open(filename, "r")
	i = 1
	datafile_header = f.read(4096 - 12)
	print_datafile_header(datafile_header)
	indexpage_header = f.read(12)
	print_indexpage_header(indexpage_header)
	while True:
		page_header = f.read(16)
		if page_header == "":
			return
		if len(page_header) < 16:
			print("Error")
			return
		page_size, = struct.unpack("<I", page_header[:4])
		if page_size < 65536:
			print("Error pageSize = %d" % (page_size))
			return
		page_data = f.read(page_size - 16)
		print_page(page_header, page_data)

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
	function(sys.argv[2])