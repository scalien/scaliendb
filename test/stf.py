#!/usr/bin/env python
"""
Scalien Test Framework

This tool can be used for testing whole test files or individual 
test functions as well.

Usage:

	stf.py <TestFileName> [TestFunctionName]

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

class Compiler:
	def __init__(self, include_dirs, build_dir):
		self.cc = "/usr/bin/g++"
		self.cflags = "-Wall -O2 "
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
	def __init__(self, ldpaths, ldlibs, build_dir):
		self.ld = "g++"
		self.ldflags = ""
		self.ldpaths = ldpaths
		self.ldlibs = ldlibs
		self.build_dir = build_dir
	
	def link(self, objfile):
		ldpaths = prefix_each(" -L", self.ldpaths)
		ldlibs = prefix_each(" -l", self.ldlibs)
		objects = find_objects(self.build_dir, ["Main.o", "ScalienDB"], ["Test"])
		objects.append(self.build_dir + "/Test/Test.o")
		objects.append(objfile)
		objects = " ".join(objects)
		output = self.build_dir + "/Test/TestProgram" #+ str(uuid.uuid1())
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

def create_main(file, func):
	main = """
	#define TEST_NAME "%s"
	#include "Test/Test.h"
	TEST_DECLARE(%s);
	TEST_MAIN(%s);
	#undef TEST_MAIN
	#define TEST_MAIN(...)
	#include "%s"
	""" % (func, func, func, file)
	f = tempfile.NamedTemporaryFile(suffix=".cpp")
	#print(main)
	f.write(main)
	f.flush()
	return f

def test_run(file, func = None):
	cc = Compiler([SRC_DIR], BUILD_DIR)
	if func == None:
		input = SRC_DIR + "/" + TEST_DIR + file + ".cpp"
		output = BUILD_DIR + "/" + TEST_DIR + file + ".o"
	else:
		cpp = TEST_DIR + file + ".cpp"
		f = create_main(cpp, func)
		input = f.name
		output = BUILD_DIR + "/" + TEST_DIR + "TestMain.o"
	cc.add_cflag("-DTEST_FILE")
	obj = cc.compile(input, output)
	ld = Linker(LDPATH, LDLIBS, BUILD_DIR)
	bin = ld.link(obj)
	#print(bin)
	shell_exec(bin, False, False)

if __name__ == "__main__":
	func = None
	if len(sys.argv) > 2:
		func = sys.argv[2]
	test_run(sys.argv[1], func)
