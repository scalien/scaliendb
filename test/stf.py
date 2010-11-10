#!/usr/bin/env python
"""
Scalien Test Framework

This tool can be used for testing whole test files or individual 
test functions as well.

Usage:

	stf.py <TestFileName> [TestFunctionName, [TestFunctionName2 [, ...]]

TestFileName must be given without path and extension e.g.

	stf.py StorageTest TestStorageCapacity
		
"""
import subprocess
from subprocess import PIPE
import tempfile
import uuid
import time
import shlex
import sys
import os

#LDPATH=["/opt/local/lib/db46", "/usr/local/lib/db4"]
LDPATH=["/opt/local/lib/db46"]
LDLIBS=["db_cxx", "pthread"]
BUILD_DIR="build/Release"
SRC_DIR="src"
TEST_DIR="Test/"
DEBUGGER="gdb -ex r"

class Compiler:
	def __init__(self, include_dirs, build_dir):
		self.cc = "/usr/bin/g++"
		self.cflags = "-Wall -O2 -D__STDC_FORMAT_MACROS "
		self.include_dirs = include_dirs
		self.build_dir = build_dir

	def add_cflag(self, cflag):
		self.cflags += cflag + " "

	def compile(self, cfile, output):
		include_dirs = prefix_each(" -I", self.include_dirs)
		cmd = self.cc + " " + self.cflags + " " + include_dirs + " -o " + output + " -c " + cfile
		ret, stdout, stderr = shell_exec(cmd)
		if ret != 0:
			print("Compiler error:")
			print("===============\n" + stderr)
			sys.exit(1)
		return output

class Linker:
	def __init__(self, ldpaths, ldlibs):
		self.ld = "g++"
		self.ldflags = ""
		self.ldpaths = ldpaths
		self.ldlibs = ldlibs
	
	def link(self, objects, output):
		ldpaths = prefix_each(" -L", self.ldpaths)
		ldlibs = prefix_each(" -l", self.ldlibs)
		objects = " ".join(objects)
		cmd = self.ld + " " + self.ldflags + " " + ldpaths + ldlibs + " -o " + output + " " + objects
		#print(cmd)
		try:
			os.remove(output)
		except OSError, e:
			pass
		try:
			ret, stdout, stderr = shell_exec(cmd)
			if ret != 0:
				print("Linker error:\n" + stderr)
				sys.exit(1)
		except OSError, e:
			pass
		return output
	
def prefix_each(prefix, list):
	out = prefix.join(list)
	if out != "": out = prefix + out
	return out

def find_objects(root_dir, exclude_files, exclude_dirs):
	objects = []
	path = root_dir
	exclude_files = set(exclude_files)
	exclude_dirs = set(exclude_dirs)
	for root, dirs, files in os.walk(root_dir):
		#print(root, dirs, files)
		for file in files:
			if file not in exclude_files:
				objects.append(os.path.join(root, file))
		for dir in dirs:
			if dir in exclude_dirs:
				dirs.remove(dir)
	return objects

def shell_exec(cmd, redirect_output = True, quiet = True):
	#print(cmd)
	args = shlex.split(cmd)
	stdout = ""
	stderr = ""
	if redirect_output:
		p = subprocess.Popen(args, stdout=PIPE, stderr=PIPE)
		output = p.communicate()
		stdout, stderr = output
		ret = p.poll()
	else:
		p = subprocess.Popen(args)
		ret = p.wait()
	if quiet == False:
		if ret < 0:
			print("Command terminated by signal (%d)" % (ret))
			print("\n\t" + cmd)
			if stdout != "":
				print("\nStdout:")
				print(stdout)
			if stderr != "":
				print("\nStderr:\n\n" + stderr)
		if ret > 0:
			print("Command aborted, (%d)" % (ret))
			print("\n\t" + cmd)
			if stdout != "":
				print("\nStdout:")
				print(stdout)
			if stderr != "":
				print("\nStderr:\n\n" + stderr)
		if ret == 0 and stderr != "":
			print(stderr)
	return ret, stdout, stderr

def tmpfile(name):
	pass

def create_main(file, testname, funcs):
	funcs_str = ");TEST_ADD(".join(funcs)
	main = """
	#define TEST_NAME "%s"
	#include "Test/TestFunction.h"
	TEST_START(main);
	TEST_LOG_INIT(LOG_TARGET_STDOUT);
	TEST_ADD(%s);
	TEST_EXECUTE();
	#undef TEST_MAIN
	#define TEST_MAIN(...)
	#include "%s"
	""" % (testname, funcs_str, file)
	f = tempfile.NamedTemporaryFile(suffix=".cpp")
	#print(main)
	f.write(main)
	f.flush()
	return f

def test_run(file, funcs = None):
	cc = Compiler([SRC_DIR], BUILD_DIR)
	if funcs == None:
		input = SRC_DIR + "/" + TEST_DIR + file + ".cpp"
		output = BUILD_DIR + "/" + TEST_DIR + file + ".o"
	else:
		cpp = TEST_DIR + file + ".cpp"
		f = create_main(cpp, file, funcs)
		input = f.name
		output = BUILD_DIR + "/" + TEST_DIR + "__TestMain.o"
	#cc.add_cflag("-DTEST_FILE")
	obj = cc.compile(input, output)
	ld = Linker(LDPATH, LDLIBS)
	objects = find_objects(BUILD_DIR, ["Main.o", "ScalienDB", "TestMain", "TestMain.o", "Test.o", "__TestMain.o", file + ".o"], [])
	objects.append(BUILD_DIR + "/" + TEST_DIR + "Test.o")
	objects.append(obj)
	output = BUILD_DIR + "/" + TEST_DIR + "TestProgram" #+ str(uuid.uuid1())
	bin = ld.link(objects, output)
	#print(bin)
	ret, stdout, stderr = shell_exec(bin, False, False)
	return ret

if __name__ == "__main__":
	func = None
	if len(sys.argv) > 2:
		funcs = sys.argv[2:]
	sys.exit(test_run(sys.argv[1], funcs))
