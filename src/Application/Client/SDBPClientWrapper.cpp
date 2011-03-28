#include "SDBPClientWrapper.h"
#include "SDBPClient.h"

using namespace SDBPClient;

/*
===============================================================================================

 NodeParams functions

===============================================================================================
*/

SDBP_NodeParams::SDBP_NodeParams(int nodec_)
{
    Log_Trace("nodec = %d", nodec_);
    num = 0;
    nodec = nodec_;
    nodes = new char*[nodec];
    for (int i = 0; i < nodec; i++)
        nodes[i] = NULL;
}

SDBP_NodeParams::~SDBP_NodeParams()
{
    Close();
}

void SDBP_NodeParams::Close()
{
	for (int i = 0; i < num; i++)
		free(nodes[i]);
	delete[] nodes;
	nodes = NULL;
	num = 0;
}

void SDBP_NodeParams::AddNode(const std::string& node)
{
	if (num > nodec)
		return;
    Log_Trace("num = %d, node = %s", num, node.c_str());
	nodes[num++] = strdup(node.c_str());
}

SDBP_Buffer::SDBP_Buffer()
{
    data = NULL;
    len = 0;
}

void SDBP_Buffer::SetBuffer(char* data_, int len_)
{
    data = data_;
    len = len_;
}

/*
===============================================================================================

 Result functions

===============================================================================================
*/

void SDBP_ResultClose(ResultObj result_)
{
    Result* result = (Result*) result_;
    
    delete result;
}

std::string SDBP_ResultKey(ResultObj result_)
{
    Result*     result = (Result*) result_;
    std::string ret;
    ReadBuffer  key;
    int         status;
    
    if (!result)
        return ret;
    
    status = result->GetKey(key);
    if (status < 0)
        return ret;
    
    ret.append(key.GetBuffer(), key.GetLength());
    
    return ret;
}

std::string SDBP_ResultValue(ResultObj result_)
{
    Result*     result = (Result*) result_;
    std::string ret;
    ReadBuffer  value;
    int         status;
    
    if (!result)
        return ret;
    
    status = result->GetValue(value);
    if (status < 0)
        return ret;
    
    ret.append(value.GetBuffer(), value.GetLength());
    
    return ret;
}

SDBP_Buffer SDBP_ResultKeyBuffer(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  key;
    int         status;
    SDBP_Buffer ret;
    
    if (!result)
        return ret;
    
    status = result->GetKey(key);
    if (status < 0)
        return ret;
    
    ret.data = key.GetBuffer();
    ret.len = key.GetLength();
    
    return ret;
}

SDBP_Buffer SDBP_ResultValueBuffer(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  value;
    int         status;
    SDBP_Buffer ret;
    
    if (!result)
        return ret;
    
    status = result->GetValue(value);
    if (status < 0)
        return ret;
    
    ret.data = value.GetBuffer();
    ret.len = value.GetLength();
    
    return ret;
}

uint64_t SDBP_ResultNumber(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  value;
    uint64_t    number;
    int         status;
    
    if (!result)
        return 0;
    
    status = result->GetNumber(number);
    if (status < 0)
        return 0;
    
    return number;
}

uint64_t SDBP_ResultDatabaseID(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  value;
    uint64_t    databaseID;
    int         status;
    
    if (!result)
        return 0;
    
    status = result->GetDatabaseID(databaseID);
    if (status < 0)
        return 0;
    
    return databaseID;
}

uint64_t SDBP_ResultTableID(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  value;
    uint64_t    tableID;
    int         status;
    
    if (!result)
        return 0;
    
    status = result->GetTableID(tableID);
    if (status < 0)
        return 0;
    
    return tableID;
}

void SDBP_ResultBegin(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return;
    
    result->Begin();
}

void SDBP_ResultNext(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return;
    
    result->Next();
}

bool SDBP_ResultIsEnd(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return true;
    
    return result->IsEnd();
}

int SDBP_ResultTransportStatus(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->GetTransportStatus();
}

int SDBP_ResultCommandStatus(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->GetCommandStatus();
}

/*
===============================================================================================

 Client functions

===============================================================================================
*/

ClientObj SDBP_Create()
{
    return new Client();
}

int SDBP_Init(ClientObj client_, const SDBP_NodeParams& params)
{
    Client* client = (Client*) client_;
    
    return client->Init(params.nodec, (const char**) params.nodes);
}

void SDBP_Destroy(ClientObj client_)
{
    Client* client = (Client*) client_;
    
    delete client;
}

ResultObj SDBP_GetResult(ClientObj client_)
{
    Client* client = (Client*) client_;
    
    return client->GetResult();
}

void SDBP_SetGlobalTimeout(ClientObj client_, uint64_t timeout)
{
    Client* client = (Client*) client_;

    client->SetGlobalTimeout(timeout);
}

void SDBP_SetMasterTimeout(ClientObj client_, uint64_t timeout)
{
    Client* client = (Client*) client_;

    client->SetMasterTimeout(timeout);
}

uint64_t SDBP_GetGlobalTimeout(ClientObj client_)
{
    Client* client = (Client*) client_;

    return client->GetGlobalTimeout();
}

uint64_t SDBP_GetMasterTimeout(ClientObj client_)
{
    Client* client = (Client*) client_;

    return client->GetMasterTimeout();
}

uint64_t SDBP_GetCurrentDatabaseID(ClientObj client_)
{
    Client* client = (Client*) client_;

    return client->GetCurrentDatabaseID();
}

uint64_t SDBP_GetCurrentTableID(ClientObj client_)
{
    Client* client = (Client*) client_;

    return client->GetCurrentTableID();
}

void SDBP_SetBatchLimit(ClientObj client_, uint64_t batchLimit)
{
    Client* client = (Client*) client_;

    return client->SetBatchLimit(batchLimit);
}

void SDBP_SetBulkLoading(ClientObj client_, bool bulk)
{
    Client* client = (Client*) client_;

    return client->SetBulkLoading(bulk);
}

int SDBP_CreateQuorum(ClientObj client_, const SDBP_NodeParams& params)
{
    Client*                 client = (Client*) client_;
    List<uint64_t>          nodes;
    uint64_t                nodeID;
    unsigned                nread;
    
    for (int i = 0; i < params.nodec; i++)
    {
        nodeID = BufferToUInt64(params.nodes[i], strlen(params.nodes[i]), &nread);
        nodes.Append(nodeID);
    }
    
    return client->CreateQuorum(nodes);
}

int SDBP_DeleteQuorum(ClientObj client_, uint64_t quorumID)
{
    Client*     client = (Client*) client_;

    return client->DeleteQuorum(quorumID);
}

int SDBP_AddNode(ClientObj client_, uint64_t quorumID, uint64_t nodeID)
{
    Client*     client = (Client*) client_;

    return client->AddNode(quorumID, nodeID);
}

int SDBP_RemoveNode(ClientObj client_, uint64_t quorumID, uint64_t nodeID)
{
    Client*     client = (Client*) client_;

    return client->RemoveNode(quorumID, nodeID);
}

int SDBP_ActivateNode(ClientObj client_, uint64_t nodeID)
{
    Client*     client = (Client*) client_;

    return client->ActivateNode(nodeID);
}

int SDBP_CreateDatabase(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());
    
    return client->CreateDatabase(name);
}

int SDBP_RenameDatabase(ClientObj client_, uint64_t databaseID, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());
    
    return client->RenameDatabase(databaseID, name);
}

int SDBP_DeleteDatabase(ClientObj client_, uint64_t databaseID)
{
    Client*     client = (Client*) client_;
    
    return client->DeleteDatabase(databaseID);
}

int SDBP_CreateTable(ClientObj client_, uint64_t databaseID, uint64_t quorumID, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());

    return client->CreateTable(databaseID, quorumID, name);
}

int SDBP_RenameTable(ClientObj client_,uint64_t tableID, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());

    return client->RenameTable(tableID, name);
}

int SDBP_DeleteTable(ClientObj client_, uint64_t tableID)
{
    Client*     client = (Client*) client_;

    return client->DeleteTable(tableID);
}

int SDBP_TruncateTable(ClientObj client_, uint64_t tableID)
{
    Client*     client = (Client*) client_;

    return client->TruncateTable(tableID);
}

uint64_t SDBP_GetDatabaseID(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());
    uint64_t    databaseID;
    int         ret;

    ret = client->GetDatabaseID(name, databaseID);
    if (ret != SDBP_SUCCESS)
        return 0;
    
    return databaseID;
}

uint64_t SDBP_GetTableID(ClientObj client_, uint64_t databaseID, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());
    uint64_t    tableID;
    int         ret;

    ret = client->GetTableID(name, databaseID, tableID);
    if (ret != SDBP_SUCCESS)
        return 0;
    
    return tableID;
}

int SDBP_UseDatabase(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());

    return client->UseDatabase(name);
}

int SDBP_UseTable(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name((char*) name_.c_str(), name_.length());

    return client->UseTable(name);
}

int	SDBP_Get(ClientObj client_, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Get(key);
}

int	SDBP_GetCStr(ClientObj client_, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Get(key);
}

int	SDBP_Set(ClientObj client_, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->Set(key, value);
}

int	SDBP_SetCStr(ClientObj client_, char* key_, int lenKey, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    value.Wrap((char*) value_, lenValue);

    return client->Set(key, value);
}

int	SDBP_SetIfNotExists(ClientObj client_, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->SetIfNotExists(key, value);
}

int	SDBP_SetIfNotExistsCStr(ClientObj client_, char* key_, int lenKey, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    value.Wrap((char*) value_, lenValue);

    return client->SetIfNotExists(key, value);
}

int	SDBP_TestAndSet(ClientObj client_, const std::string& key_, const std::string& test_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  test((char*) test_.c_str(), test_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->TestAndSet(key, test, value);
}

int	SDBP_TestAndSetCStr(ClientObj client_, char* key_, int lenKey, char* test_, int lenTest, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  test;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    test.Wrap((char*) test_, lenTest);
    value.Wrap((char*) value_, lenValue);

    return client->TestAndSet(key, test, value);
}

int	SDBP_GetAndSet(ClientObj client_, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->GetAndSet(key, value);
}

int	SDBP_GetAndSetCStr(ClientObj client_, char* key_, int lenKey, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    value.Wrap((char*) value_, lenValue);

    return client->GetAndSet(key, value);
}

int SDBP_Add(ClientObj client_, const std::string& key_, int64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Add(key, number);
}

int SDBP_AddCStr(ClientObj client_, char* key_, int len, int64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Add(key, number);
}

int SDBP_Append(ClientObj client_, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->Append(key, value);
}

int SDBP_AppendCStr(ClientObj client_, char* key_, int lenKey, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    value.Wrap((char*) value_, lenValue);

    return client->Append(key, value);
}

int SDBP_Delete(ClientObj client_, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Delete(key);
}

int SDBP_DeleteCStr(ClientObj client_, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Delete(key);
}

int SDBP_Remove(ClientObj client_, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Remove(key);
}

int SDBP_RemoveCStr(ClientObj client_, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Remove(key);
}

int SDBP_ListKeys(ClientObj client_, const std::string& key_, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->ListKeys(key, count, offset);
}

int SDBP_ListKeysCStr(ClientObj client_, char* key_, int len, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->ListKeys(key, count, offset);
}

int SDBP_ListKeyValues(ClientObj client_, const std::string& key_, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->ListKeyValues(key, count, offset);
}

int SDBP_ListKeyValuesCStr(ClientObj client_, char* key_, int len, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->ListKeyValues(key, count, offset);
}

int SDBP_Count(ClientObj client_, const std::string& key_, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Count(key, count, offset);
}

int SDBP_CountCStr(ClientObj client_, char* key_, int len, unsigned count, unsigned offset)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Count(key, count, offset);
}

/*
===============================================================================================

 Grouping commands

===============================================================================================
*/

int SDBP_Begin(ClientObj client_)
{
    Client*     client = (Client*) client_;
    
    return client->Begin();
}

int SDBP_Submit(ClientObj client_)
{
    Client*     client = (Client*) client_;
    
    return client->Submit();
}

int SDBP_Cancel(ClientObj client_)
{
    Client*     client = (Client*) client_;
    
    return client->Cancel();
}

bool SDBP_IsBatched(ClientObj client_)
{
    Client*     client = (Client*) client_;
    
    return client->IsBatched();
}

/*
===============================================================================================

 Debugging

===============================================================================================
*/

void SDBP_SetTrace(bool trace)
{
	if (trace)
	{
		Log_SetTrace(true);
        Log_SetTimestamping(true);
		Log_SetTarget(LOG_TARGET_STDOUT);
	}
	else
	{
		Log_SetTrace(false);
		Log_SetTarget(LOG_TARGET_NOWHERE);
	}
}
