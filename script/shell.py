"""
ScalienDB CLI shell

Usage: cli [optinal args]

Args:
    -d <database>       Initial database to use
    -t <table>          Initial table to use
    -h <hostname>       Hostname to connect to
    --trace		Turns on trace
"""
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
            elif opt == "-trace":
                scaliendb.set_trace(True)
        else:
            print(__doc__)
            sys.exit(1)
    return defaults

# helper function for measure timing
def timer(func, *args):
    starttime = time.time()
    ret = func(*args)
    endtime = time.time()
    elapsed = "(%.02f secs)" % float(endtime-starttime)
    print(elapsed)
    return ret

class Func:
    def __init__(self, name, function):
        self.name = name
        self.function = function
        self.__doc__ = self.function.__doc__
        
    def __call__(self, *args):
        return self.function(*args)
    
    def __repr__(self):
        if self.__doc__ == None:
            return "For more info on " + self.name + ", see http://scalien.com/documentation"
        return self.__doc__

class TimerFunc(Func):
    def __call__(self, *args):
        try:
            ret = timer(self.function, *args)
            globals()["result"] = client._result
            return ret
        except scaliendb.Error as e:
            print(e)    


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

# import json module that is used with config state related functions
if try_import("json"):
    def show_tables():
        """ Shows tables in current database """
        config = json.loads(client.get_json_config_state())
        for table in config["tables"]:
            if table["databaseID"] == client.get_current_database_id():
                print("name: " + table["name"] + ", id: " + str(table["tableID"]))
    def show_quorums():
        """ Shows quorums """
        config = json.loads(client.get_json_config_state())
        for quorum in config["quorums"]:
            print("id: " + str(quorum["quorumID"]) + ", primaryID: " + str(quorum["primaryID"]) + ", paxosID: " + str(quorum["paxosID"]) +
             ", active nodes: " + str(quorum["activeNodes"]) + ", inactive nodes: " + str(quorum["inactiveNodes"]))
    def show_databases():
        """ Shows databases """
        config = json.loads(client.get_json_config_state())
        for database in config["databases"]:
            print("name: " + database["name"] + ", id: " + str(database["databaseID"]))
    globals()["show_tables"] = Func("show_tables", show_tables)
    globals()["show_quorums"] = Func("show_quorums", show_quorums)
    globals()["show_databases"] = Func("show_databases", show_databases)

if try_import("scaliendb_mapreduce"):
    globals()["mapreduce"] = scaliendb_mapreduce

del try_import
    

# helper function for making closures easier
def closure(func, *args):
    return lambda: func(*args)


# helper function for other connections
def connect(nodes, database=None, table=None):
    """
    Connects to a cluster and optionally sets the database and table
    to be used.
    
    Args:
        nodes (string or list): the controller node(s) in "hostname:port" format
        
        database (string): the name of the database to be used (default=None)
        
        table (string): the name of the table to be used (default=None)
    """
    client = scaliendb.Client(nodes)    
    globals()["client"] = client
    # import client's member functions to the global scope
    members = inspect.getmembers(client, inspect.ismethod)
    for name, func in members:
        if name[0] != "_":
            globals()[name] = TimerFunc(name, func)
    try:
        if database == None:
            return
        globals()["database"] = client.get_database(database)
        if table == None:
            return
        globals()["table"] = client.get_database(database).get_table(table)
    except scaliendb.Error as e:
        print(e)


# helper class for implementing 'shelp' command
class SHelp:
    def __repr__(self):
        output = "Client commands:\n\n"
        members = inspect.getmembers(client, inspect.ismethod)
        for name, func in members:
            if name[0] != "_":
                if func.__doc__ != None:
                    pad = "".join([" " for x in xrange(30 - len(name))])
                    doc = func.__doc__.lstrip().split("\n")[0]
                    output += name + pad + doc + "\n"
                else:
                    output += name + "\n"
        output += "\n"
        if globals().has_key("show_databases"):
            output += "Type the name of the command for help on usage\n"
            output += "Use show_databases() to show databases\n"
            output += "Use show_tables() to show tables in current database\n"            
        output += "Use connect(nodes, database, table) to connect to the cluster\n"
        output += str(quit)
        return output


# helper function for welcome message
def welcome():
    header = "ScalienDB shell " + scaliendb._get_version()
    line = "".join(["=" for x in xrange(len(header))])
    print(line + "\n" + header + "\n" + line + "\n")
    print("This is a standard Python shell, enhanced with ScalienDB client library commands.")
    print("Type \"shelp\" for help.\n")

# create default client
defaults = parse_args(sys.argv)

welcome()
del welcome

# register 'shelp' command
shelp = SHelp()

connect = Func("connect", connect)
connect(defaults.nodes, defaults.database, defaults.table)
