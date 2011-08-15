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
        i = Client._typemap(a)
        c = "%s/%s" %  (c, i)
    return c
    	        
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

# =============================================================================================
#
# Error
#
# =============================================================================================

class Error(Exception):
    def __init__(self, status, strerror=None):
        self.status = status
        self.strerror = strerror
    
    def __str__(self):
        if self.strerror != None:
            return str_status(self.status) + ": " + self.strerror
        return str_status(self.status)

# =============================================================================================
#
# Client
#
# =============================================================================================

class Client:

    # =========================================================================================
    #
    # Result
    #
    # =========================================================================================

    class Result:
        def __init__(self, cptr):
            self._cptr = cptr

        def __del__(self):
            self.close()
        
        def __iter__(self):
            self.begin()
            return self
        
        def close(self):
            SDBP_ResultClose(self._cptr)
            self._cptr = None
        
        def key(self):
            return SDBP_ResultKey(self._cptr)
        
        def value(self):
            return SDBP_ResultValue(self._cptr)

        def signed_number(self):
            return SDBP_ResultSignedNumber(self._cptr)
        
        def number(self):
            return SDBP_ResultNumber(self._cptr)
        
        def begin(self):
            return SDBP_ResultBegin(self._cptr)
        
        def is_end(self):
            return SDBP_ResultIsEnd(self._cptr)
        
        def is_finished(self):
            return SDBP_ResultIsFinished(self._cptr)
                
        def next(self):
            if self.is_end():
                raise StopIteration
            SDBP_ResultNext(self._cptr)
            return self
        
        def command_status(self):
            return SDBP_ResultCommandStatus(self._cptr)
        
        def transport_status(self):
            return SDBP_ResultTransportStatus(self._cptr)
        
        def connectivity_status(self):
            return SDBP_ResultConnectivityStatus(self._cptr)
        
        def timeout_status(self):
            return SDBP_ResultTimeoutStatus(self._cptr)
        
        def num_nodes(self):
            return SDBP_ResultNumNodes(self._cptr)
        
        def node_id(self, node=0):
            return SDBP_ResultNodeID(self._cptr, node)
        
        def elapsed_time(self):
            return SDBP_ResultElapsedTime(self._cptr)
        
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
    
    # =========================================================================================
    #
    # Quorum
    #
    # =========================================================================================

    class Quorum:
        def __init__(self, client, quorum_id, name):
            self._client = client
            self._quorum_id = quorum_id
            self.name = name

    # =========================================================================================
    #
    # Database
    #
    # =========================================================================================

    class Database:
        def __init__(self, client, database_id, name):
            self._client = client
            self._database_id = database_id
            self.name = name

        def get_table(self, name):
            tables = self.get_tables()
            for table in tables:
                if table.name == name:
                    return table;
            return None

        def get_tables(self):
            num_tables = SDBP_GetNumTables(self._client._cptr, self._database_id)
            tables = []
            for i in xrange(num_tables):
                tableID = SDBP_GetTableIDAt(self._client._cptr, self._database_id, i)
                name = SDBP_GetTableNameAt(self._client._cptr, self._database_id, i)
                tables.append(Client.Table(self._client, self, tableID, name))
            return tables
        
        def create_table(self, name, quorum=None):
            if quorum is None:
                quorums = self._client.get_quorums()
                if len(quorums) < 1:
                    raise Error(SDBP_BADSCHEMA, "No quorums")
                quorum = quorums[0]
            status = SDBP_CreateTable(self._client._cptr, self._database_id, quorum._quorum_id, name)
            self._client._result = Client.Result(SDBP_GetResult(self._client._cptr))
            self._client._check_status(status)
            return self.get_table(name)
        
        def rename_database(self, name):
            status = SDBP_RenameDatabase(self._client._cptr, self._database_id, name)
            self._client._check_status(status)
            self.name = name

        def delete_database(self):
            status = SDBP_DeleteDatabase(self._client._cptr, self._database_id)
            self._client._check_status(status)


    # =========================================================================================
    #
    # Table
    #
    # =========================================================================================

    class Table:
        def __init__(self, client, database, table_id, name):
            self._client = client
            self.database = database
            self._table_id = table_id
            self.name = name

        def rename_table(self, name):
            status = SDBP_RenameTable(self._client._cptr, self._table_id, name)
            self._client._check_status(status)
            self.name = name
        
        def delete_table(self):
            status = SDBP_DeleteTable(self._client._cptr, self._table_id)
            self._client._check_status(status)
        
        def truncate_table(self):
            status = SDBP_TruncateTable(self._client._cptr, self._table_id)
            self._client._check_status(status)
                        
        def get(self, key):
            return self._client._get(self._table_id, key)

        def set(self, key, value):
            return self._client._set(self._table_id, key, value)
        
        def delete(self, key):
            return self._client._delete(self._table_id, key)

        def count(self, prefix="", start_key="", end_key=""):
            return self._client._count(self._table_id, prefix, start_key, end_key)
        
        def get_key_iterator(self, prefix="", start_key="", end_key=""):
            return self._client.Iterator(self, prefix, start_key, end_key, False)
    
        def get_key_value_iterator(self, prefix="", start_key="", end_key=""):
            return self._client.Iterator(self, prefix, start_key, end_key, True)
            
        def get_sequence(self, key):
            return self._client.Sequence(self, key)
            

    # =========================================================================================
    #
    # Iterator
    #
    # =========================================================================================

    class Iterator:
        def __init__(self, table, prefix, start_key, end_key, values):
            self._table = table
            self._start_key = start_key
            self._end_key = end_key
            self._prefix = prefix
            self._values = values
            self._count = 100
            self._result = []
            self._pos = 0
            self._len = 0

        def _query(self, skip):
            if self._values:
                self._result = self._table._client._list_key_values(self._table._table_id, self._prefix, self._start_key, self._end_key, self._count, skip).items()
            else:
                self._result = self._table._client._list_keys(self._table._table_id, self._prefix, self._start_key, self._end_key, self._count, skip)
            self._result.sort()
            self._len = len(self._result)
            self._pos = 0
        
        def __iter__(self):
            return self

        def next(self):
            if self._len == 0:
                self._query(False)
            else:
                self._pos += 1
                if self._pos == self._len:
                    if self._len < self._count:
                        raise StopIteration
                    if self._values:
                        self._start_key = self._result[self._len-1][0]
                    else:
                        self._start_key = self._result[self._len-1]
                    self._query(True)                        
            if self._len == 0:
                raise StopIteration
            return self._result[self._pos]

    # =============================================================================================
    #
    # Sequence
    #
    # =============================================================================================

    class Sequence:
        def __init__(self, table, key):
            self._table = table
            self._key = key
            self._gran = 1000
            self._seq = 0
            self._num = 0
        
        def set_granularity(self, gran):
            self._gran = 1000
        
        def reset():
            client._set(self._table._table_id, self._key, 0)
        
        def get(self):
            if self._num == 0:
                self._allocate_range()    
            self._num -= 1
            ret = self._seq
            self._seq += 1
            return ret
        
        def _allocate_range(self):
            self._seq = self._table._client._add(self._table._table_id, self._key, self._gran) - self._gran
            self._num = self._gran
        

    # =============================================================================================
    #
    # Client
    #
    # =============================================================================================

    def __init__(self, nodes):
        self._cptr = SDBP_Create()
        self._result = None
        if isinstance(nodes, list):
            node_params = SDBP_NodeParams(len(nodes))
            for node in nodes:
                node_params.AddNode(node)
            SDBP_Init(self._cptr, node_params)
            node_params.Close()
        elif isinstance(nodes, str):
            node_params = SDBP_NodeParams(1)
            node_params.AddNode(nodes)
            SDBP_Init(self._cptr, node_params)
            node_params.Close()
        else:
            raise Error(SDBP_API_ERROR, "nodes argument must be string or list")

    def __del__(self):
        del self._result
        SDBP_Destroy(self._cptr)

    def set_global_timeout(self, timeout):
        SDBP_SetGlobalTimeout(self._cptr, long(timeout))
    
    def set_master_timeout(self, timeout):
        SDBP_SetMasterTimeout(self._cptr, long(timeout))

    def get_global_timeout(self):
        return long(SDBP_GetGlobalTimeout(self._cptr))
        
    def get_master_timeout(self):
        return long(SDBP_GetMasterTimeout(self._cptr))

    def set_consistency_mode(self, consistency_mode):
        SDBP_SetConsistencyMode(self._cptr, consistency_mode)

    def set_batch_mode(self, batch_mode):
        SDBP_SetBatchMode(self._cptr, batch_mode)

    def set_batch_limit(self, limit):
        SDBP_SetBatchLimit(self._cptr, long(limit))

    def get_json_config_state(self):
        """ Returns the config state in JSON string """
        return SDBP_GetJSONConfigState(self._cptr)

    def get_quorum(self, name):
        quorums = self.get_quorums()
        for quorum in quorums:
            if quorum.name == name:
                return quorum
        return None

    def get_quorums(self):
        num_quorums = SDBP_GetNumQuorums(self._cptr)
        quorums = []
        for i in xrange(num_quorums):
            quorum_id = SDBP_GetQuorumIDAt(self._cptr, i)
            name = SDBP_GetQuorumNameAt(self._cptr, i)
            quorum = self.Quorum(self, quorum_id, name)
            quorums.append(quorum)
        return quorums

    def get_database(self, name):
        databases = self.get_databases()
        for db in databases:
            if db.name == name:
                return db
        return None

    def get_databases(self):
        num_databases = SDBP_GetNumDatabases(self._cptr)
        databases = []
        for i in xrange(num_databases):
            database_id = SDBP_GetDatabaseIDAt(self._cptr, i)
            name = SDBP_GetDatabaseNameAt(self._cptr, i)
            db = self.Database(self, database_id, name)
            databases.append(db)
        return databases
    
    def create_database(self, name):
        status = SDBP_CreateDatabase(self._cptr, name)
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        self._check_status(status)
        return self.get_database(name)
    
    def _get(self, table_id, key):
        key = Client._typemap(key)
        status, ret = self._data_command(SDBP_Get, table_id, key)
        if ret:
            return self._result.value()

    def _set(self, table_id, key, value):
        key = Client._typemap(key)
        value = Client._typemap(value)
        status, ret = self._data_command(SDBP_Set, table_id, key, value)
        if status == SDBP_FAILURE:
            raise Error(status, "Set failed")
        
    def _add(self, table_id, key, num):
        key = Client._typemap(key)
        status, ret = self._data_command(SDBP_Add, table_id, key, num)
        if ret:
            return self._result.signed_number()

    def _delete(self, table_id, key):
        key = Client._typemap(key)
        status, ret = self._data_command(SDBP_Delete, table_id, key)

    def _count(self, table_id, prefix="", start_key="", end_key=""):
        start_key = Client._typemap(start_key)
        end_key = Client._typemap(end_key)
        status = SDBP_Count(self._cptr, table_id, start_key, end_key, prefix)
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        self._check_status(status)
        return self._result.number()

    def _list_keys(self, table_id, prefix="", start_key="", end_key="", count=0, skip=False):
        start_key = Client._typemap(start_key)
        end_key = Client._typemap(end_key)
        status = SDBP_ListKeys(self._cptr, table_id, start_key, end_key, prefix, count, skip)
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        self._check_status(status)
        keys = []
        self._result.begin()
        while not self._result.is_end():
            keys.append(self._result.key())
            self._result.next()
        return keys

    def _list_key_values(self, table_id, prefix="", start_key="", end_key="", count=0, skip=False):
        start_key = Client._typemap(start_key)
        end_key = Client._typemap(end_key)
        status = SDBP_ListKeyValues(self._cptr, table_id, start_key, end_key, prefix, count, skip)
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        self._check_status(status)
        key_values = {}
        self._result.begin()
        while not self._result.is_end():
            key_values[self._result.key()] = self._result.value()
            self._result.next()
        return key_values
    
    def submit(self):
        status = SDBP_Submit(self._cptr)
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        if status == SDBP_PARTIAL:
            raise Error(status, "Not all request could be served")
        if status == SDBP_FAILURE:
            raise Error(status, "No request could be served")
        return status
    
    def rollback(self):
        return SDBP_Cancel(self._cptr)
    
    def _data_command(self, func, *args):
        status = func(self._cptr, *args)
        if status < 0:
            self._result = Client.Result(SDBP_GetResult(self._cptr))
            if status == SDBP_API_ERROR:
                raise Error(status, "Maximum request limit exceeded")
            if status == SDBP_BADSCHEMA:
                raise Error(status, "No database or table is in use")
            if status == SDBP_NOSERVICE:
                raise Error(status, "No server in the cluster was able to serve the request")
            return status, False
        self._result = Client.Result(SDBP_GetResult(self._cptr))
        return status, True
        
    def _check_status(self, status):
        if status < 0:
            if status == SDBP_BADSCHEMA:
                raise Error(status, "No database or table is in use")
            if status == SDBP_NOSERVICE:
                raise Error(status, "No server in the cluster was able to serve the request")
            raise Error(status)
    
    @staticmethod
    def _typemap(i):
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


def set_trace(trace=True):
    SDBP_SetTrace(trace)

def _get_version():
    return SDBP_GetVersion()

def _get_debug_string():
    return SDBP_GetDebugString()

def _human_bytes(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
