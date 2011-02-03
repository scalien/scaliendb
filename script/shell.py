# enable syntax completion
try:
    import readline
except ImportError:
    print("Module readline not available.")
else:
    import rlcompleter
    readline.parse_and_bind("tab: complete")
import scaliendb
print("\nScalienDB shell\n")
test_client = scaliendb.Client(["localhost:7080"])
