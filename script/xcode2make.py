import os
import re

def seekto(f, pattern):
	while True:
		line = f.readline()
		if line == "":
			return False
		if line.strip() == pattern:
			return line

def seekto_regex(f, pattern):
	while True:
		line = f.readline()
		if line == "":
			return False
		m = re.search(pattern, line.strip())
		if m != None:
			return m

def extract_pbx_target(line):
	m = re.search(r"^\s+\w+ /\* (.*\.(c|cpp)) in .*", line)
	return m.group(1)

def read_pbxproject_targets(pbxfile):
	targets = set()
	f = open(pbxfile, "r")
	seekto_regex(f, r"buildConfigurationList = \w+ /\* Build configuration list for PBXNativeTarget \"ScalienDB\" \*/");
	seekto_regex(f, r"buildPhases = \(")
	m = seekto_regex(f, "(\w+) /\* Sources \*/")
	srcid = m.group(1)
	seekto(f, "/* Begin PBXSourcesBuildPhase section */")
	seekto(f, srcid + " /* Sources */ = {")
	seekto(f, "files = (")
	while True:
		line = f.readline()
		if line == "":
			return False
		if line.strip() == ");":
			return targets
		targets.add(extract_pbx_target(line))

def find_source_files(root_dir, exclude_files, exclude_dirs):
	objects = set()
	path = root_dir
	exclude_files = set(exclude_files)
	exclude_dirs = set(exclude_dirs)
	for root, dirs, files in os.walk(root_dir):
		#print(root, dirs, files)
		for file in files:
			if file not in exclude_files:
				if file.find(".cpp") == len(file) - 4 or file.find(".c") == len(file) - 2:
					objects.add(os.path.join(root, file))
		for dir in dirs:
			if dir in exclude_dirs:
				dirs.remove(dir)
	return objects

def match_targets_with_source_files(pbxtargets, srcfiles):
	objects = set()
	for target in pbxtargets:
		for srcfile in srcfiles:
			pos = srcfile.find(target)
			if pos != -1 and pos == len(srcfile) - len(target):
				obj = srcfile.replace(".cpp", ".o").replace(".c", ".o")
				objects.add(obj)
	return objects
	
def print_makefile_objects(build_dir, objects):
	print("ALL_OBJECTS = \\")
	for obj in sorted(objects):
		print("\t" + obj.replace(build_dir, "$(BUILD_DIR)") + " \\")
	print("\n")

if __name__ == "__main__":
	pbxfile = "ScalienDB.xcodeproj/project.pbxproj"
	pbxtargets = read_pbxproject_targets(pbxfile)
	srcfiles = find_source_files("src", ["TestMain.cpp", "Main.cpp"], ["StorageOld"])
	makefile_objs = match_targets_with_source_files(pbxtargets, srcfiles)
	print_makefile_objects("src", makefile_objs)
