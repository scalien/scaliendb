import sys
import scaliendb
import inspect
import time

class Defaults:
    def __init__(self):
        self.database = None
        self.table = None
        self.nodes = "localhost:7080"

def parse_args(args):
    defaults = Defaults()
    skip = True
    for x in xrange(len(args)):
        if skip:
            skip = False
            continue
        if args[x][0] == "-":
            opt = args[x][1:]
            if opt == "d":
                defaults.database = args[x + 1]
                skip = True
            elif opt == "t":
                defaults.table = args[x + 1]
                skip = True
            elif opt == "h":
                defaults.nodes = args[x + 1]
                skip = True
    return defaults

# helper function for trying to import other useful modules
def try_import(name):
    try:
        globals()[name] = __import__(name)
        return True
    except:
        print("Module " + name + " not available.")
        return False

print("")

# enable syntax completion
if try_import("readline"):
    try_import("rlcompleter")
    if sys.platform == "darwin":
        # this works on Snow Leopard
        readline.parse_and_bind ("bind ^I rl_complete")
    else:
        # this works on Linux    
        readline.parse_and_bind("tab: complete")

if try_import("json"):
    def show_tables():
        config = json.loads(client.get_json_config_state())
        for table in config["tables"]:
            if table["databaseID"] == client.get_current_database_id():
                print("name: " + table["name"] + ", id: " + str(table["tableID"]))
    globals()["show_tables"] = show_tables

del try_import
    
# helper function for making closures easier
def closure(func, *args):
    return lambda: func(*args)

# helper function for measure timing
def timer(func, *args):
    starttime = time.time()
    ret = func(*args)
    endtime = time.time()
    elapsed = "(%.02f secs)" % float(endtime-starttime)
    print(elapsed)
    return ret

# helper function for other connections
def connect(nodes, database=None, table=None):
    def timer_func(f):
        def func(*args):
            try:
                ret = timer(f, *args)
                globals()["result"] = client.result
                return ret
            except scaliendb.Error as e:
                print(e)
        return func
    client = scaliendb.Client(nodes)    
    globals()["client"] = client
    # import client's member functions to the global scope
    members = inspect.getmembers(client, inspect.ismethod)
    for k, v in members:
        if k[0] != "_":
            globals()[k] = timer_func(v)
    try:
        if database == None:
            return
        client.use_database(database)
        if table == None:
            return
        client.use_table(table)
    except scaliendb.Error as e:
        print(e)

# helper class for implementing 'shelp' command
class SHelp:
    def __repr__(self):
        output = "Use connect(nodes, database, table) to connect to the cluster\n"
        output += str(quit)
        return output


# helper function for welcome message
def welcome():
    header = "ScalienDB shell " + scaliendb.get_version()
    line = "".join(["=" for x in xrange(len(header))])
    print("\n" + line + "\n" + header + "\n" + line + "\n")
    print("This is a standard Python shell, enhanced with ScalienDB client library commands.")
    print("Type \"shelp\" for help.\n")

welcome()
del welcome

# register 'shelp' command
shelp = SHelp()

# create default client
defaults = parse_args(sys.argv)
connect(defaults.nodes, defaults.database, defaults.table)
