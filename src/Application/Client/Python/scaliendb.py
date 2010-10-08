from scaliendb_client import *

SDBP_SUCCESS = 0
SDBP_API_ERROR = -1

SDBP_PARTIAL = -101
SDBP_FAILURE = -102

SDBP_NOMASTER = -201
SDBP_NOCONNECTION =	-202

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
		
		def close(self):
			SDBP_ResultClose(self.cptr)
			self.cptr = None
		
		def key(self):
			return SDBP_ResultKey(self.cptr)
		
		def value(self):
			return SDBP_ResultValue(self.cptr)
		
		def begin(self):
			return SDBP_ResultBegin(self.cptr)
		
		def is_end(self):
			return SDBP_ResultIsEnd(self.cptr)
		
		def next(self):
			return SDBP_ResultNext(self.cptr)
		
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

	def get_database_id(self, name):
		return long(SDBP_GetDatabaseID(self.cptr, name))
	
	def get_table_id(self, database_id, name):
		return long(SDBP_GetTableID(self.cptr, database_id, name))
	
	def get(self, database_id, table_id, key):
		status = SDBP_Get(self.cptr, database_id, table_id, key)
		if status < 0:
			self.result = Client.Result(SDBP_GetResult(self.cptr))
			return
		if SDBP_IsBatched(self.cptr):
			return
		self.result = Client.Result(SDBP_GetResult(self.cptr))
		return self.result.value()

	def set(self, database_id, table_id, key, value):
		status = SDBP_Set(self.cptr, database_id, table_id, key, value)
		if status < 0:
			self.result = Client.Result(SDBP_GetResult(self.cptr))
			return
		if SDBP_IsBatched(self.cptr):
			return
		self.result = Client.Result(SDBP_GetResult(self.cptr))
		return status

	def delete(self, database_id, table_id, key):
		status = SDBP_Delete(self.cptr, database_id, table_id, key)
		if status < 0:
			self.result = Client.Result(SDBP_GetResult(self.cptr))
			return
		if SDBP_IsBatched(self.cptr):
			return
		self.result = Client.Result(SDBP_GetResult(self.cptr))
		return status
		
	def _set_trace(self, trace):
		SDBP_SetTrace(trace)

		
