import os
import re

def find_source_dirs(root_dir, exclude_dirs):
	srcdirs = []
	path = root_dir
	exclude_dirs = set(exclude_dirs)
	for root, dirs, files in os.walk(root_dir):
		#print(root, dirs, files)
		for dir in dirs:
			if dir in exclude_dirs:
				dirs.remove(dir)
			else:
				srcdirs.append(root + "/" + dir)
	return srcdirs

def print_makefile_dirs(build_dir, dirs):
	print("BUILD_DIRS = \\")
	for dir in sorted(dirs):
		print("\t\t" + dir.replace(build_dir, "$(BUILD_DIR)") + " \\")
	print("\n")
	print("$(BUILD_DIR):")
	print("\t-mkdir -p $(BUILD_DIR) \\")
	print("\t\t$(BIN_DIR) \\")
	print("\t\t$(BUILD_DIRS)")
	print("\n")

if __name__ == "__main__":
	srcdirs = find_source_dirs("src", [])
	print_makefile_dirs("src", srcdirs)
