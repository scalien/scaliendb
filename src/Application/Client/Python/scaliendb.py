from scaliendb_client import *

SDBP_SUCCESS = 0
SDBP_API_ERROR = -1

SDBP_PARTIAL = -101
SDBP_FAILURE = -102

SDBP_NOMASTER = -201
SDBP_NOCONNECTION = -202

SDBP_MASTER_TIMEOUT = -301
SDBP_GLOBAL_TIMEOUT = -302

SDBP_NOSERVICE = -401
SDBP_FAILED = -402

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
    elif status == SDBP_MASTER_TIMEOUT:
        return "SDBP_MASTER_TIMEOUT"
    elif status == SDBP_GLOBAL_TIMEOUT:
        return "SDBP_GLOBAL_TIMEOUT"
    elif status == SDBP_NOSERVICE:
        return "SDBP_NOSERVICE"
    elif status == SDBP_FAILED:
        return "SDBP_FAILED"

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
        
        def key_values(self):
            kv = {}
            self.begin()
            while not self.is_end():
                kv[self.key()] = self.value()
                self.next()
            return kv
    
    def __init__(self, nodes):
        self.cptr = SDBP_Create()
        self.result = None
        self.database_id = 0
        self.table_id = 0
        node_params = SDBP_NodeParams(len(nodes))
        for node in nodes:
            node_params.AddNode(node)
        SDBP_Init(self.cptr, node_params)
        node_params.Close()

    def __del__(self):
        del self.result
        SDBP_Destroy(self.cptr)

    def set_global_timeout(self, timeout):
        SDBP_SetGlobalTimeout(self.cptr, long(timeout))
    
    def get_global_timeout(self):
        return long(SDBP_GetGlobalTimeout(self.cptr))
    
    def set_master_timeout(self, timeout):
        SDBP_SetMasterTimeout(self.cptr, long(timeout))
    
    def get_master_timeout(self):
        return long(SDBP_GetMasterTimeout(self.cptr))

    def create_quorum(self, nodes):
        node_params = SDBP_NodeParams(len(nodes))
        for node in nodes:
            node_params.AddNode(node)
        status = SDBP_CreateQuorum(self.cptr, node_params)
        node_params.Close()
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        return self.result.number()
    
    def activate_node(self, node_id):
        status = SDBP_ActivateNode(self.cptr, node_id)
        return status
    
    def create_database(self, name):
        status = SDBP_CreateDatabase(self.cptr, name)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        return self.result.number()

    def create_table(self, database_id, quorum_id, name):
        status = SDBP_CreateTable(self.cptr, database_id, quorum_id, name)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        return self.result.number()

    def truncate_table(self, table_id):
        status = SDBP_TruncateTable(self.cptr, table_id)
        return status

    def get_database_id(self, name):
        return long(SDBP_GetDatabaseID(self.cptr, name))
    
    def get_table_id(self, database_id, name):
        return long(SDBP_GetTableID(self.cptr, database_id, name))
    
    def use_database(self, name):
        return SDBP_UseDatabase(self.cptr, name)
    
    def use_table(self, name):
        return SDBP_UseTable(self.cptr, name)
    
    def get(self, key):
        status = SDBP_Get(self.cptr, key)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return self.result.value()

    def set(self, key, value):
        status = SDBP_Set(self.cptr, key, value)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status

    def set_if_not_exists(self, key, value):
        status = SDBP_SetIfNotExists(self.cptr, key, value)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status

    def test_and_set(self, key, test, value):
        status = SDBP_TestAndSet(self.cptr, key, test, value)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status

    def get_and_set(self, key, value):
        status = SDBP_GetAndSet(self.cptr, key, value)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return self.result.value()
        
    def add(self, key, value):
        status = SDBP_Add(self.cptr, key, value)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return self.result.number()

    def delete(self, key):
        status = SDBP_Delete(self.cptr, key)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status

    def remove(self, key):
        status = SDBP_remove(self.cptr, key)
        if status < 0:
            self.result = Client.Result(SDBP_GetResult(self.cptr))
            return
        if SDBP_IsBatched(self.cptr):
            return
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return self.result.value()

    def list_keys(self, key, count=0, offset=0):
        status = SDBP_ListKeys(self.cptr, key, count, offset)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        keys = []
        self.result.begin()
        while not self.result.is_end():
            keys.append(self.result.key())
            self.result.next()
        return keys

    def list_key_values(self, key, count=0, offset=0):
        status = SDBP_ListKeyValues(self.cptr, key, count, offset)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        key_values = {}
        self.result.begin()
        while not self.result.is_end():
            key_values[self.result.key()] = self.result.value()
            self.result.next()
        return key_values

    def begin(self):
        return SDBP_Begin(self.cptr)
    
    def submit(self):
        status = SDBP_Submit(self.cptr)
        self.result = Client.Result(SDBP_GetResult(self.cptr))
        return status
    
    def cancel(self):
        return SDBP_Cancel(self.cptr)
    
    def is_batched(self):
        return SDBP_IsBatched(self.cptr)
        
    def _set_trace(self, trace):
        SDBP_SetTrace(trace)

        
