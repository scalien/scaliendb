from scaliendb_client import *
import time
from datetime import datetime
from datetime import date

SDBP_SUCCESS = 0
SDBP_API_ERROR = -1

SDBP_PARTIAL = -101
SDBP_FAILURE = -102

SDBP_NOMASTER = -201
SDBP_NOCONNECTION = -202
SDBP_NOPRIMARY = -203

SDBP_MASTER_TIMEOUT = -301
SDBP_GLOBAL_TIMEOUT = -302
SDBP_PRIMARY_TIMEOUT = -303

SDBP_NOSERVICE = -401
SDBP_FAILED = -402
SDBP_BADSCHEMA = -403

SDBP_CONSISTENCY_ANY = 0
SDBP_CONSISTENCY_RYW = 1
SDBP_CONSISTENCY_STRICT = 2

SDBP_BATCH_DEFAULT = 0
SDBP_BATCH_NOAUTOSUBMIT	= 1
SDBP_BATCH_SINGLE = 2

def composite(*args):
    c = ""
    for a in args:
        i = Util.typemap(a)
        c = "%s/%s" %  (c, i)
    return c

class Callable:
    def __init__(self, anycallable):
        self.__call__ = anycallable

class Util:
	def typemap(i):
		if isinstance(i, (str)):
			return i
		elif isinstance(i, (int, long)):
			return "%021d" % i
		elif isinstance(i, (datetime)):
			return i.strftime("%Y-%m-%d %H:%M:%S.%f")
		elif isinstance(i, (date)):
			return i.strftime("%Y-%m-%d")
		else:
			return str(i)
	typemap = Callable(typemap)
	        
def str_status(status):
    if status == SDBP_SUCCESS:
        return "SDBP_SUCCESS"
    elif status == SDBP_API_ERROR:
        return "SDBP_API_ERROR"
    elif status == SDBP_PARTIAL:
        return "SDBP_PARTIAL"
    elif status == SDBP_FAILURE:
        return "SDBP_FAILURE"
    elif status == SDBP_NOMASTER:
        return "SDBP_NOMASTER"
    elif status == SDBP_NOCONNECTION:
        return "SDBP_NOCONNECTION"
    elif status == SDBP_NOPRIMARY:
        return "SDBP_NOPRIMARY"
    elif status == SDBP_MASTER_TIMEOUT:
        return "SDBP_MASTER_TIMEOUT"
    elif status == SDBP_GLOBAL_TIMEOUT:
        return "SDBP_GLOBAL_TIMEOUT"
    elif status == SDBP_PRIMARY_TIMEOUT:
        return "SDBP_PRIMARY_TIMEOUT"
    elif status == SDBP_NOSERVICE:
        return "SDBP_NOSERVICE"
    elif status == SDBP_FAILED:
        return "SDBP_FAILED"
    elif status == SDBP_BADSCHEMA:
        return "SDBP_BADSCHEMA"
    return "<UNKNOWN>"

class Error(Exception):
    def __init__(self, status, strerror=None):
        self.status = status
        self.strerror = strerror
    
    def __str__(self):
        if self.strerror != None:
            return str_status(self.status) + ": " + self.strerror
        return str_status(self.status)

class Client:

    class Result:
        def __init__(self, cptr):
            self.cptr = cptr

        def __del__(self):
            self.close()
        
        def __iter__(self):
            self.begin()
            return self
        
        def close(self):
            SDBP_ResultClose(self.cptr)
            self.cptr = None
        
        def key(self):
            return SDBP_ResultKey(self.cptr)
        
        def value(self):
            return SDBP_ResultValue(self.cptr)

        def signed_number(self):
            return SDBP_ResultSignedNumber(self.cptr)
        
        def number(self):
            return SDBP_ResultNumber(self.cptr)
        
        def database_id(self):
            return SDBP_ResultDatabaseID(self.cptr)
        
        def table_id(self):
            return SDBP_ResultTableID(self.cptr)

        def begin(self):
            return SDBP_ResultBegin(self.cptr)
        
        def is_end(self):
            return SDBP_ResultIsEnd(self.cptr)
        
        def is_finished(self):
            return SDBP_ResultIsFinished(self.cptr)
        
        def is_conditional_success(self):
            return SDBP_ResultIsConditionalSuccess(self.cptr)
        
        def next(self):
            if self.is_end():
                raise StopIteration
            SDBP_ResultNext(self.cptr)
            return self
        
        def command_status(self):
            return SDBP_ResultCommandStatus(self.cptr)
        
        def transport_status(self):
            return SDBP_ResultTransportStatus(self.cptr)
        
        def connectivity_status(self):
            return SDBP_ResultConnectivityStatus(self.cptr)
        
        def timeout_status(self):
            return SDBP_ResultTimeoutStatus(self.cptr)
        
        def num_nodes(self):
            return SDBP_ResultNumNodes(self.cptr)
        
        def node_id(self, node=0):
            return SDBP_ResultNodeID(self.cptr, node)
        
        def elapsed_time(self):
            return SDBP_ResultElapsedTime(self.cptr)
        
        def keys(self):
            keys = []
            self.begin()
            while not self.is_end():
                keys.append(self.key())
                self.next()
            return keys
        
        def key_values(self):
            kv = {}
            self.begin()
            while not self.is_end():
                kv[self.key()] = self.value()
                self.next()
            return kv
            
        def map(self, mapfunc):
            num = 0
            last_key = None
            self.begin()
            while not self.is_end():
                last_key = self.key()
                mapfunc(self.key(), self.value())
                self.next()
                num += 1
            return num, last_key
    
    def __init__(self, nodes):
        self.cptr = SDBP_Create()
        self.result = None
        self.database_id = 0
        self.table_id = 0
        if isinstance(nodes, list):
            node_params = SDBP_NodeParams(len(nodes))
            for node in nodes:
                node_params.AddNode(node)
            SDBP_Init(self.cptr, node_params)
            node_params.Close()
        elif isinstance(nodes, str):
            node_params = SDBP_NodeParams(1)
            node_params.AddNode(nodes)
            SDBP_Init(self.cptr, node_params)
            node_params.Close()
        else:
            raise Error(SDBP_API_ERROR, "nodes argument must be string or list")

    class Quorum:
        def __init__(self, client, quorum_name, quorum_id):
            self.client = client
            self.quorum_name = quorum_name
            self.quorum_id = quorum_id

    class Table:
        def __init__(self, client, database_name, table_name):
            self.client = client
            self.database_name = database_name
            self.table_name = table_name
        
        def use_defaults(self):
            self.client.use_database(self.database_name)
            self.client.use_table(self.table_name)
        
        def truncate(self):
            self.client.truncate_table(self.table_name)
            
        def get(self, key):
            self.use_defaults()
            return self.client.get(key)

        def set(self, key, value):
            self.use_defaults()
            return self.client.set(key, value)
        
        def add(self, key, num):
            self.use_defaults()
            return self.client.add(key, num)

        def delete(self, key):
            self.use_defaults()
            return self.client.delete(key)

        def list_keys(self, start_key="", end_key="", prefix="", count=0, offset=0):
            self.use_defaults()
            return self.client.list_keys(start_key, end_key, prefix, count, offset)

        def list_key_values(self, start_key="", end_key="", prefix="", count=0, offset=0):
            self.use_defaults()
            return self.client.list_key_values(start_key, end_key, prefix, count, offset)

        def count(self, start_key="", end_key="", prefix="", count=0, offset=0):
            self.use_defaults()
            return self.client.count(start_key, end_key, prefix, count, offset)

    class Database:
        def __init__(self, client, database_name):
            self.client = client
            self.database_name = database_name
        
        def get_tables(self):
            return self.client.get_tables(self.database_name)
        
        def get_table(self, table_name):
            return self.client.get_table(self.database_name, table_name)
        
        def exists_table(self, table_name):
            table_names = self.get_tables()
            if table_name in table_names:
                return True
            else:
                return False
        
        def create_table(self, table_name, quorum_name=None):
            self.client.use_database(self.database_name)
            return self.client.create_table(table_name, quorum_name)
        
        def create_table_cond(self, table_name, quorum_name=None):
            self.client.use_database(self.database_name)
            if self.exists_table(table_name):
                return self.get_table(table_name)
            return self.client.create_table(table_name, quorum_name)
        
        def create_empty_table_cond(self, table_name, quorum_name=None):
            self.client.use_database(self.database_name)
            if self.exists_table(table_name):
                self.truncate_table(table_name)
                return self.get_table(table_name)
            return self.client.create_table(table_name, quorum_name)

        def delete_table(self, table_name):
            self.client.use_database(self.database_name)
            self.client.delete_table(table_name)

        def truncate_table(self, table_name):
            self.client.use_database(self.database_name)
            self.client.truncate_table(table_name)

    def __del__(self):
        del self.result
        SDBP_Destroy(self.cptr)

    def set_global_timeout(self, timeout):
        """
        Sets the global timeout
        
        Args:
            timeout (long): the global timeout in milliseconds
        """
        SDBP_SetGlobalTimeout(self.cptr, long(timeout))
    
    def get_global_timeout(self):
        """ Returns the global timeout in milliseconds """
        return long(SDBP_GetGlobalTimeout(self.cptr))
    
    def set_master_timeout(self, timeout):
        """
        Sets the master timeout
        
        Args:
            timeout (long): the master timeout in milliseconds
        """
        SDBP_SetMasterTimeout(self.cptr, long(timeout))
    
    def get_master_timeout(self):
        """ Returns the master timeout in milliseconds """
        return long(SDBP_GetMasterTimeout(self.cptr))

    def set_consistency_level(self, consistency_level):
        """
        Sets the consistency level
        
        Args:
            consistency_level (int): can be any of SDBP_CONSISTENCY_ANY, SDBP_CONSISTENCY_RYW,
                                     or SDBP_CONSISTENCY_STRICT
        """
        SDBP_SetConsistencyLevel(self.cptr, consistency_level)

    def set_batch_mode(self, batch_mode):
        """
        Sets batch mode
        
        Args:
            mode (int): can be any of SDBP_BATCH_DEFAULT, SDBP_BATCH_NOAUTOSUBMIT,
                        or SDBP_BATCH_SINGLE
        """
        SDBP_SetBatchMode(self.cptr, batch_mode)

    def set_batch_limit(self, limit):
        """
        Sets batch limit
        
        Args:
            limit (long): the maximum amount of bytes that could be put in a batch
        """
        SDBP_SetBatchLimit(self.cptr, long(limit))

    def get_json_config_state(self):
        """ Returns the config state in JSON string """
        return SDBP_GetJSONConfigState(self.cptr)

    def wait_config_state(self):
        """ Waits until config state is updated """
        SDBP_WaitConfigState(self.cptr)

    def create_quorum(self, nodes, name=""):
        """
        Creates a quorum
        
        Args:
            nodes (list): the ids of the nodes that constitutes the quorum
            name (string): the name of the quorum
        """
        node_params = SDBP_NodeParams(len(nodes))
        for node in nodes:
            node_params.AddNode(str(node))
        status = SDBP_CreateQuorum(self.cptr, name, node_params)
        node_params.Close()
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        quorum_id = self.result.number();
        quorum_name = self.get_quorum_name(quorum_id)
        return self.Quorum(self, quorum_name, quorum_id)

    def create_quorum_cond(self, nodes, name=""):
        """
        Creates a quorum if it does not exists
        
        Args:
            nodes (list): the ids of the nodes that constitutes the quorum

            name (string): the name of the quorum
        """
        if self.exists_quorum(name):
           return self.get_quorum(name)
        return self.create_quorum(nodes, name)
    
    def rename_quorum(self, quorum, new_name):
        """
        Deletes a quorum
        
        Args:
            quorum (string or Quorum): the quorum to be renamed

            new_name (string): the new name of the quorum
        """
        if isinstance(quorum, (str)):
            quorum_id = self.get_quorum_id(quorum)
        else:
            quorum_id = self.get_quorum_id(quorum.quorum_name)
        status = SDBP_RenameQuorum(self.cptr, quorum_id, new_name)
        self._check_status(status)    
    
    def delete_quorum(self, quorum):
        """
        Deletes a quorum
        
        Args:
            quorum (string or Quorum): the quorum
        """
        if isinstance(quorum, (str)):
            quorum_id = self.get_quorum_id(quorum)
        else:
            quorum_id = self.get_quorum_id(quorum.quorum_name)
        status = SDBP_DeleteQuorum(self.cptr, quorum_id)
        self._check_status(status)

    def add_node(self, quorum_id, node_id):
        """
        Adds a node to a qourum
        
        Args:
            quorum_id (long): the id of the quorum
        
            node_id (long): the id of the node
        """
        status = SDBP_AddNode(self.cptr, quorum_id, node_id)
        self._check_status(status)

    def remove_node(self, quorum_id, node_id):
        """
        Removes a node to a qourum
        
        Args:
            quorum_id (long): the id of the quorum
        
            node_id (long): the id of the node
        """
        status = SDBP_RemoveNode(self.cptr, quorum_id, node_id)
        self._check_status(status)
    
    def activate_node(self, node_id):
        """
        Activates a node
        
        Args:
            node_id (long): the id of the node
        """
        status = SDBP_ActivateNode(self.cptr, node_id)
        self._check_status(status)
    
    def create_database(self, name):
        """
        Creates a database
        
        Args:
            name (string): the name of the database to be created
        """
        status = SDBP_CreateDatabase(self.cptr, name)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        return self.get_database(name)

    def create_database_cond(self, name):
        """
        Creates a database if it does not exists
        
        Args:
            name (string): the name of the database to be created
        """
        if self.exists_database(name):
           return self.get_database(name)
        return self.create_database(name)

    def rename_database(self, src, dst):
        """
        Renames a database
        
        Args:
            src (string): the name of the database to be renamed
            
            dst (string): the new name of the database
        """
        database_id = self.get_database_id(src)
        status = SDBP_RenameDatabase(self.cptr, database_id, dst)
        self._check_status(status)

    def delete_database(self, name):
        """
        Deletes a database
        
        Args:
            name (string): the name of the database
        """
        database_id = self.get_database_id(name)
        status = SDBP_DeleteDatabase(self.cptr, database_id)
        self._check_status(status)

    def create_table(self, name, quorum=None):
        """
        Creates a table
        
        Args:
            name (string): the name of the table

            quorum (string or Quorum): the quorum to put the first shard (OPTIONAL)            
        """
        if quorum is None:
            quorums = self.get_quorums()
            if len(quorums) < 1:
                raise Error(SDBP_BADSCHEMA, "No quorums")
            quorum_id = quorums[0].quorum_id
        else:
            if isinstance(quorum, (str)):
                quorum_id = self.get_quorum_id(quorum)
            else:
                quorum_id = self.get_quorum_id(quorum.quorum_name)
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        print(quorum_id)
        status = SDBP_CreateTable(self.cptr, database_id, quorum_id, name)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        database_name = self.get_database_name(database_id)
        return self.get_table(database_name, name)

    def rename_table(self, src, dst):
        """
        Renames a table
        
        Args:
            src (string): the name of the table to be renamed
            
            dst (string): the new name of the table
        """
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        table_id = self.get_table_id(database_id, src)
        self.rename_table_by_id(table_id, dst)

    def rename_table_by_id(self, table_id, name):
        """
        Renames a table
        
        Args:
            table_id (long): the id of the table to be renamed
            
            name (string): the new name of the table
        """
        status = SDBP_RenameTable(self.cptr, table_id, name)
        if status < 0:
            if status == SDBP_FAILED:
                raise Error(status, "No table found")
            raise Error(status)

    def delete_table(self, name):
        """
        Deletes a table
        
        Args:
            name (string): the name of the table
        """
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        table_id = self.get_table_id(database_id, name)
        self.delete_table_by_id(table_id)

    def delete_table_by_id(self, table_id):
        """
        Deletes a table
        
        Args:
            table_id (long): the id of the table
        """
        status = SDBP_DeleteTable(self.cptr, table_id)
        if status < 0:
            if status == SDBP_FAILED:
                raise Error(status, "No table found")
            raise Error(status)

    def truncate_table(self, name):
        """
        Truncates a table
        
        Args:
            name (string): the name of the table
        """
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        table_id = self.get_table_id(database_id, name)
        self.truncate_table_by_id(table_id)

    def truncate_table_by_id(self, table_id):
        """
        Truncates a table
        
        Args:
            table_id (long): the id of the table
        """
        status = SDBP_TruncateTable(self.cptr, table_id)
        if status < 0:
            if status == SDBP_FAILED:
                raise Error(status, "No table found")
            raise Error(status)

    def split_shard(self, shard_id, key=None):
        """
        Splits a shard
        
        Args:
            shard_id (long): the id of the shard
            
            key (string): optional, the key where the shard is to be splitted
        """
        key = Util.typemap(key)
        if key == None:
            status = SDBP_SplitShardAuto(self.cptr, shard_id)
        else:
            status = SDBP_SplitShard(self.cptr, shard_id, key)
        if status < 0:
            raise Error(status)
    
    def freeze_table(self, name):
        """
        Freezes a table
        
        Args:
            name (string): the name of the table
        """
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        table_id = self.get_table_id(database_id, name)
        self.freeze_table_by_id(table_id)

    def freeze_table_by_id(self, table_id):
        """
        Freezes a table
        
        Args:
            table_id (long): the id of the table
        """
        status = SDBP_FreezeTable(self.cptr, table_id)
        if status < 0:
            raise Error(status)
    
    def unfreeze_table(self, name):
        """
        Unfreezes a table
        
        Args:
            name (string): the name of the table
        """
        database_id = long(SDBP_GetCurrentDatabaseID(self.cptr))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database selected")
        table_id = self.get_table_id(database_id, name)
        self.unfreeze_table_by_id(table_id)

    def unfreeze_table_by_id(self, table_id):
        """
        Unfreezes a table
        
        Args:
            table_id (long): the id of the table
        """
        status = SDBP_UnfreezeTable(self.cptr, table_id)
        if status < 0:
            raise Error(status)
    
    def migrate_shard(self, quorum, shard_id):
        """
        Migrates a shard to a given quorum
        
        Args:
            quorum (string or Quorum): the quorum to migrate to            
            shard_id (long): the id of the shard
        """
        if isinstance(quorum, (str)):
            quorum_id = self.get_quorum_id(quorum)
        else:
            quorum_id = self.get_quorum_id(quorum.quorum_name)
        status = SDBP_MigrateShard(self.cptr, quorum_id, shard_id)
        if status < 0:
            raise Error(status)

    def get_database_id(self, name):
        """
        Returns the id of a database
        
        Args:
            name (string): the name of the database
        """
        database_id = long(SDBP_GetDatabaseID(self.cptr, name))
        if database_id == 0:
            raise Error(SDBP_BADSCHEMA, "No database found with name '%s'" % (name))
        return database_id

    def get_database_name(self, database_id):
        """
        Returns the name of a database
        
        Args:
            database_id (long): the id of the database
        """
        database_name = SDBP_GetDatabaseName(self.cptr, database_id)
        if database_name == "":
            raise Error(SDBP_BADSCHEMA, "No database found with id '%s'" % (database_id))
        return database_name
    
    def get_table_id(self, database_id, name):
        """
        Returns the id of a table
        
        Args:
            database_id (long): the id of the database that contains the given table

            name (string): the name of the table
        """
        table_id = long(SDBP_GetTableID(self.cptr, database_id, name))
        if table_id == 0:
            raise Error(SDBP_BADSCHEMA, "No table found with name '%s'" % (name))
        return table_id
    
    def get_current_database_id(self):
        """ Returns the current database id """
        return long(SDBP_GetCurrentDatabaseID(self.cptr))
    
    def get_current_table_id(self):
        """ Returns the current table id """
        return long(SDBP_GetCurrentTableID(self.cptr))

    def exists_quorum(self, quorum_name):
        quorums = self.get_quorums()
        for quorum in quorums:
            if quorum.name == quorum_name:
                return True
        return False

    def get_quorums(self):
        num_quorums = SDBP_GetNumQuorums(self.cptr)
        quorums = []
        for i in xrange(num_quorums):
            quorum_id = SDBP_GetQuorumIDAt(self.cptr, i)
            quorum_name = SDBP_GetQuorumNameAt(self.cptr, i)
            quorum = self.Quorum(self, quorum_name, quorum_id)
            quorums.append(quorum)
        return quorums

    def get_quorum_name(self, quorum_id):
        num_quorums = SDBP_GetNumQuorums(self.cptr)
        quorum_names = []
        for i in xrange(num_quorums):
            id = SDBP_GetQuorumIDAt(self.cptr, i)
            quorum_name = SDBP_GetQuorumNameAt(self.cptr, i)
            if id == quorum_id:
                return quorum_name
        raise Error(status, "No such quorum id!")

    def get_quorum_id(self, name):
        num_quorums = SDBP_GetNumQuorums(self.cptr)
        quorum_names = []
        for i in xrange(num_quorums):
            quorum_id = SDBP_GetQuorumIDAt(self.cptr, i)
            quorum_name = SDBP_GetQuorumNameAt(self.cptr, i)
            if quorum_name == name:
                return quorum_id
        raise Error(status, "No such quorum name!")

    def get_databases(self):
        num_databases = SDBP_GetNumDatabases(self.cptr)
        databases = []
        for i in xrange(num_databases):
            database = SDBP_GetDatabaseNameAt(self.cptr, i)
            databases.append(database)
        return databases

    def get_database_ids(self):
        num_databases = SDBP_GetNumDatabases(self.cptr)
        databases = []
        for i in xrange(num_databases):
            database_id = SDBP_GetDatabaseIDAt(self.cptr, i)
            database_name = SDBP_GetDatabaseNameAt(self.cptr, i)
            database = {"database_id": database_id, "name": database_name}
            databases.append(database)
        return databases
        
    def exists_database(self, database_name):
        database_names = self.get_databases()
        if database_name in database_names:
            return True
        else:
            return False
    
    def use_database(self, name):
        """
        Uses a database. All following operations will be executed on that database.
        
        Args:
            name (string): the name of the database
        """
        status = SDBP_UseDatabase(self.cptr, name)
        if status != SDBP_SUCCESS:
            if status == SDBP_NOSERVICE:
                raise Error(status, "Cannot connect to controller!")
            raise Error(status, "No database found with name '%s'" % (name))

    def use_database_id(self, id):
        """
        Uses a database. All following operations will be executed on that database.
        
        Args:
            id (int): the id of the database
        """
        status = SDBP_UseDatabaseID(self.cptr, id)
        if status != SDBP_SUCCESS:
            if status == SDBP_NOSERVICE:
                raise Error(status, "Cannot connect to controller!")
            raise Error(status, "No database found with id '%s'" % (id))

    def get_tables(self, database_name):
        self.use_database(database_name)
        num_tables = SDBP_GetNumTables(self.cptr)
        tables = []
        for i in xrange(num_tables):
            table = SDBP_GetTableNameAt(self.cptr, i)
            tables.append(table)
        return tables
    
    def use_table(self, name):
        """
        Uses a table. All following operations will be executed on that table.
        
        Args:
            name (string): the name of the table
        """
        status = SDBP_UseTable(self.cptr, name)
        if status != SDBP_SUCCESS:
            raise Error(SDBP_BADSCHEMA, "No table found with name '%s'" % (name))            

    def use_table_id(self, id):
        """
        Uses a table. All following operations will be executed on that table.
        
        Args:
            id (int): the name of the table
        """
        status = SDBP_UseTableID(self.cptr, id)
        if status != SDBP_SUCCESS:
            raise Error(SDBP_BADSCHEMA, "No table found with id '%s'" % (id))            
    
    
    def use(self, database, table=None):
        """
        Uses a database and table.
        
        Args:
            database (string): the name of the database
            
            table (string): optional, the name of the table
        """
        self.use_database(database)
        if table != None:
            self.use_table(table)
    
    def get(self, key):
        """
        Returns the value for a specified key
        
        Args:
            key (string): the specified key
        """
        key = Util.typemap(key)
        status, ret = self._data_command(SDBP_Get, key)
        if ret:
            return self.result.value()

    def set(self, key, value):
        """
        Sets the value for a specified key
        
        Args:
            key (string): the specified key
            
            value (string): the value to be set
        """
        key = Util.typemap(key)
        value = Util.typemap(value)
        status, ret = self._data_command(SDBP_Set, key, value)
        if status == SDBP_FAILURE:
            raise Error(status, "Set failed")
        
    def add(self, key, num):
        """
        Adds a specified number to the given key and returns the new value
        
        Args:
            key (string): the specified key
                        
            value (string): the value to be set
        """
        key = Util.typemap(key)
        status, ret = self._data_command(SDBP_Add, key, num)
        if ret:
            return self.result.signed_number()

    def delete(self, key):
        """
        Deletes the specified key
        
        Args:
            key (string): the specified key
        """
        key = Util.typemap(key)
        status, ret = self._data_command(SDBP_Delete, key)

    def list_keys(self, start_key="", end_key="", prefix="", count=0, offset=0):
        """
        Lists the keys of a table. Returns a list of strings.
        
        Args:
            start_key (string): the key from where the listing starts (default="")

            end_key (string): the key where the listing ends (default="")
            
            prefix (string): keys must start with prefix
            
            count (long): the maximum number of keys to be returned (default=0)
            
            offset (long): start the listing at this offset (default=0)
        """
        start_key = Util.typemap(start_key)
        end_key = Util.typemap(end_key)
        status = SDBP_ListKeys(self.cptr, start_key, end_key, prefix, count, offset)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        keys = []
        self.result.begin()
        while not self.result.is_end():
            keys.append(self.result.key())
            self.result.next()
        return keys

    def list_key_values(self, start_key="", end_key="", prefix="", count=0, offset=0):
        """
        Lists the keys and values of a table. Returns a dict of key-value pairs.
        
        Args:
            start_key (string): the key from where the listing starts (default="")

            end_key (string): the key where the listing ends (default="")

            prefix (string): keys must start with prefix
            
            count (long): the maximum number of keys to be returned (default=0)
            
            offset (long): start the listing at this offset (default=0)
        """
        start_key = Util.typemap(start_key)
        end_key = Util.typemap(end_key)
        status = SDBP_ListKeyValues(self.cptr, start_key, end_key, prefix, count, offset)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        key_values = {}
        self.result.begin()
        while not self.result.is_end():
            key_values[self.result.key()] = self.result.value()
            self.result.next()
        return key_values

    def count(self, start_key="", end_key="", prefix="", count=0, offset=0):
        """
        Counts the number of items in a table. Returns the number of found items.
        
        Args:
            start_key (string): the key from where the listing starts (default="")

            end_key (string): the key where the listing ends (default="")

            prefix (string): keys must start with prefix
            
            count (long): the maximum number of keys to be returned (default=0)
            
            offset (long): start the listing at this offset (default=0)
        """
        start_key = Util.typemap(start_key)
        end_key = Util.typemap(end_key)
        status = SDBP_Count(self.cptr, start_key, end_key, prefix, count, offset)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        self._check_status(status)
        return self.result.number()        
    
    def submit(self):
        """ Sends the batched operations and waits until all is acknowledged """
        status = SDBP_Submit(self.cptr)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status == SDBP_PARTIAL:
            raise Error(status, "Not all request could be served")
        if status == SDBP_FAILURE:
            raise Error(status, "No request could be served")
        return status
    
    def cancel(self):
        """ Cancels the batching """
        return SDBP_Cancel(self.cptr)
    
    def _data_command(self, func, *args):
        status = func(self.cptr, *args)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            if status == SDBP_API_ERROR:
                raise Error(status, "Maximum request limit exceeded")
            if status == SDBP_BADSCHEMA:
                raise Error(status, "No database or table is in use")
            if status == SDBP_NOSERVICE:
                raise Error(status, "No server in the cluster was able to serve the request")
            return status, False
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status, True
    
    def get_quorum(self, quorum_name):
        return self.Quorum(self, quorum_name, self.get_quorum_id(quorum_name))
    
    def get_table(self, database_name, table_name):
        return self.Table(self, database_name, table_name)

    def get_database(self, database_name):
        return self.Database(self, database_name)
    
    def _check_status(self, status):
        if status < 0:
            if status == SDBP_BADSCHEMA:
                raise Error(status, "No database or table is in use")
            if status == SDBP_NOSERVICE:
                raise Error(status, "No server in the cluster was able to serve the request")
            raise Error(status)

def set_trace(trace=True):
    SDBP_SetTrace(trace)

def get_version():
    return SDBP_GetVersion()

def get_debug_string():
    return SDBP_GetDebugString()

def human_bytes(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0

class Autosharding:
    def __init__(self, client):
        self.client = client
    
    def show_shardserver_stats(self):
        import json
        config = json.loads(self.client.get_json_config_state())
        for shard_server in config["shardServers"]:
            self.show_shardserver(config, shard_server)
    
    def show_shardserver(self, config, shard_server):
        size = 0
        for shard in config["shards"]:
            for quorum_info in shard_server["quorumInfos"]:
                if shard["quorumID"] == quorum_info["quorumID"]:
                    size += shard["shardSize"]
        print("Node: %d, size: %d" % (shard_server["nodeID"], size))