import os
import sys

BUFSIZE=1024*1024

class BackupFile:
    def __init__(self, file, name):
        self.file = file
        self.name = name

def strip_prefix(s, prefix):
    if s.find(prefix) == 0:
        return s[len(prefix):]
    return s

def copy_file(input, output):
    while True:
        data = input.read(BUFSIZE)
        if len(data) == 0:
            break
        output.write(data)

if __name__ == "__main__":
    root_dir = sys.argv[1]
    output_dir = sys.argv[2]
    backup_files = []

    # walk database directory
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            inputname = os.path.join(root, file)
            f = open(inputname, "rb")
            backup_file = BackupFile(f, os.path.join(output_dir, strip_prefix(inputname, root_dir)))
            backup_files.append(backup_file)
        for dir in dirs:
            if dir == "archive":
                dirs.remove(dir)

    # create output directories
    try:
        os.mkdir(output_dir)    
        os.mkdir(os.path.join(output_dir, "chunks"))
        os.mkdir(os.path.join(output_dir, "logs"))
    except OSError, e:
        if e.errno != 17: # already exists
            raise e
    
    # copy files
    for backup_file in backup_files:
        print("Copying " + backup_file.name)
        f = open(backup_file.name, "wb")
        copy_file(backup_file.file, f)
        f.close()
