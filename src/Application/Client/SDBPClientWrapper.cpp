#include "SDBPClientWrapper.h"
#include "SDBPClient.h"
#include "SDBPPooledShardConnection.h"

#include "Application/ConfigServer/JSONConfigState.h"
#include "Version.h"
#include "SourceControl.h"

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
    data = (intptr_t)(void*)NULL;
    len = 0;
}

void SDBP_Buffer::SetBuffer(char* data_, int len_)
{
    data = (intptr_t) data_;
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
    
    ret.data = (intptr_t) key.GetBuffer();
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
    
    ret.data = (intptr_t) value.GetBuffer();
    ret.len = value.GetLength();
    
    return ret;
}

int64_t SDBP_ResultSignedNumber(ResultObj result_)
{
    Result*     result = (Result*) result_;
    ReadBuffer  value;
    int64_t    number;
    int         status;
    
    if (!result)
        return 0;
    
    status = result->GetSignedNumber(number);
    if (status < 0)
        return 0;
    
    return number;
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

bool SDBP_ResultIsConditionalSuccess(ResultObj result_)
{
    Result*     result = (Result*) result_;
    int         status;
    bool        ret;
    
    if (!result)
        return 0;
    
    status = result->IsConditionalSuccess(ret);
    if (status < 0)
        return false;
    
    return ret;
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

bool SDBP_ResultIsFinished(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return true;
    
    return result->IsFinished();
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

unsigned SDBP_ResultNumNodes(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->GetNumNodes();
}

uint64_t SDBP_ResultNodeID(ResultObj result_, unsigned n)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->GetNodeID(n);
}

unsigned SDBP_ResultElapsedTime(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->GetElapsedTime();
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

std::string SDBP_GetJSONConfigState(ClientObj client_)
{
    Client*             client = (Client*) client_;
    ConfigState*        configState;
    JSONBufferWriter    jsonWriter;
    JSONConfigState     jsonConfigState;
    Buffer              buffer;
    std::string         ret;
    
    client->Lock();
    configState = client->UnprotectedGetConfigState();
    if (configState == NULL)
    {
        client->Unlock();
        return "{}";    // empty
    }
    
    jsonWriter.Init(&buffer);
    jsonWriter.Start();

    jsonConfigState.SetConfigState(configState);
    jsonConfigState.SetJSONBufferWriter(&jsonWriter);
    jsonConfigState.Write();

    jsonWriter.End();

    client->Unlock();
    
    ret.assign(buffer.GetBuffer(), buffer.GetLength());
    return ret;
}

void SDBP_WaitConfigState(ClientObj client_)
{
    Client* client = (Client*) client_;

    client->WaitConfigState();
}

void SDBP_SetConsistencyMode(ClientObj client_, int consistencyMode)
{
    Client* client = (Client*) client_;

    return client->SetConsistencyMode(consistencyMode);
}

void SDBP_SetBatchMode(ClientObj client_, int batchMode)
{
    Client* client = (Client*) client_;

    return client->SetBatchMode(batchMode);
}

void SDBP_SetBatchLimit(ClientObj client_, unsigned batchLimit)
{
    Client* client = (Client*) client_;

    return client->SetBatchLimit(batchLimit);
}

/*
===============================================================================================

 Schema commands

===============================================================================================
*/

//int SDBP_CreateQuorum(ClientObj client_, const std::string& name_, const SDBP_NodeParams& params)
//{
//    Client*                 client = (Client*) client_;
//    List<uint64_t>          nodes;
//    uint64_t                nodeID;
//    unsigned                nread;
//    ReadBuffer              name((char*) name_.c_str(), name_.length());
//
//    for (int i = 0; i < params.nodec; i++)
//    {
//        nodeID = BufferToUInt64(params.nodes[i], strlen(params.nodes[i]), &nread);
//        nodes.Append(nodeID);
//    }
//    
//    return client->CreateQuorum(name, nodes);
//}
//
//int SDBP_RenameQuorum(ClientObj client_, uint64_t quorumID, const std::string& name_)
//{
//    Client*     client = (Client*) client_;
//    ReadBuffer  name((char*) name_.c_str(), name_.length());
//    
//    return client->RenameQuorum(quorumID, name);
//}
//
//int SDBP_DeleteQuorum(ClientObj client_, uint64_t quorumID)
//{
//    Client*     client = (Client*) client_;
//
//    return client->DeleteQuorum(quorumID);
//}
//
//int SDBP_AddNode(ClientObj client_, uint64_t quorumID, uint64_t nodeID)
//{
//    Client*     client = (Client*) client_;
//
//    return client->AddNode(quorumID, nodeID);
//}
//
//int SDBP_RemoveNode(ClientObj client_, uint64_t quorumID, uint64_t nodeID)
//{
//    Client*     client = (Client*) client_;
//
//    return client->RemoveNode(quorumID, nodeID);
//}
//
//int SDBP_ActivateNode(ClientObj client_, uint64_t nodeID)
//{
//    Client*     client = (Client*) client_;
//
//    return client->ActivateNode(nodeID);
//}

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

unsigned SDBP_GetNumQuorums(ClientObj client_)
{
    Client*             client = (Client*) client_;
    
    return client->GetNumQuorums();
}

uint64_t SDBP_GetQuorumIDAt(ClientObj client_, unsigned n)
{
    Client*             client = (Client*) client_;
    
    return client->GetQuorumIDAt(n);
}

std::string SDBP_GetQuorumNameAt(ClientObj client_, unsigned n)
{
    Client*             client = (Client*) client_;
    Buffer              name;
    std::string         ret;
    
    client->GetQuorumNameAt(n, name);
    ret.assign(name.GetBuffer(), name.GetLength());
    
    return ret;
}

uint64_t SDBP_GetQuorumIDByName(ClientObj client_, const std::string& name_)
{
    Client*             client = (Client*) client_;
    ReadBuffer          name;
    uint64_t            quorumID;
    ConfigQuorum*       quorum;
    ConfigState*        configState;
    ReadBuffer          quorumName;
    
    name.Wrap((char*) name_.c_str(), name_.length());
    quorumID = 0;

    client->Lock();
    
    configState = client->UnprotectedGetConfigState();
    FOREACH (quorum, configState->quorums)
    {
        quorumName.Wrap(quorum->name);
        if (ReadBuffer::Cmp(quorumName, name) == 0)
        {
            quorumID = quorum->quorumID;
            break;
        }
    }
    
    client->Unlock();
    
    return quorumID;
    
}

unsigned SDBP_GetNumDatabases(ClientObj client_)
{
    Client*             client = (Client*) client_;
    
    return client->GetNumDatabases();
}

uint64_t SDBP_GetDatabaseIDAt(ClientObj client_, unsigned n)
{
    Client*             client = (Client*) client_;

    return client->GetDatabaseIDAt(n);
}

std::string SDBP_GetDatabaseNameAt(ClientObj client_, unsigned n)
{
    Client*             client = (Client*) client_;
    Buffer              name;
    std::string         ret;

    client->GetDatabaseNameAt(n, name);
    ret.assign(name.GetBuffer(), name.GetLength());
    
    return ret;
}

uint64_t SDBP_GetDatabaseIDByName(ClientObj client_, const std::string& name_)
{
    Client*             client = (Client*) client_;
    ConfigState*        configState;
    ConfigDatabase*     database;
    ReadBuffer          databaseName;
    ReadBuffer          name;
    uint64_t            databaseID;

    name.Wrap((char*) name_.c_str(), name_.length());
    databaseID = 0;

    client->Lock();
    
    configState = client->UnprotectedGetConfigState();
    FOREACH (database, configState->databases)
    {
        databaseName.Wrap(database->name);
        if (ReadBuffer::Cmp(databaseName, name) == 0)
        {
            databaseID = database->databaseID;
            break;
        }
    }
    
    client->Unlock();
    
    return databaseID;
}

unsigned SDBP_GetNumTables(ClientObj client_, uint64_t databaseID)
{
    Client*             client = (Client*) client_;

    return client->GetNumTables(databaseID);
}

uint64_t SDBP_GetTableIDAt(ClientObj client_, uint64_t databaseID, unsigned n)
{
    Client*             client = (Client*) client_;

    return client->GetTableIDAt(databaseID, n);
}

std::string SDBP_GetTableNameAt(ClientObj client_, uint64_t databaseID, unsigned n)
{
    Client*             client = (Client*) client_;
    Buffer              name;
    std::string         ret;
    
    client->GetTableNameAt(databaseID, n, name);    
    ret.assign(name.GetBuffer(), name.GetLength());
    
    return ret;
}

uint64_t SDBP_GetTableIDByName(ClientObj client_, uint64_t databaseID, const std::string& name_)
{
    Client*             client = (Client*) client_;
    ConfigState*        configState;
    ConfigTable*        table;
    ReadBuffer          tableName;
    ReadBuffer          name;
    uint64_t            tableID;

    name.Wrap((char*) name_.c_str(), name_.length());
    tableID = 0;

    client->Lock();
    
    configState = client->UnprotectedGetConfigState();
    FOREACH (table, configState->tables)
    {
        tableName.Wrap(table->name);
        if (table->databaseID == databaseID && ReadBuffer::Cmp(tableName, name) == 0)
        {
            tableID = table->tableID;
            break;
        }
    }
    
    client->Unlock();
    
    return tableID;    
}

//unsigned SDBP_GetNumShards(ClientObj client_)
//{
//    Client*             client = (Client*) client_;
//    ConfigState*        configState;
//    
//    configState = client->GetConfigState();
//    if (configState == NULL)
//        return 0;
//
//    return configState->shards.GetLength();
//}
//
//uint64_t SDBP_GetShardIDAt(ClientObj client_, unsigned n)
//{
//    Client*             client = (Client*) client_;
//    ConfigState*        configState;
//    ConfigShard*        configShard;
//    unsigned            i;
//    
//    configState = client->GetConfigState();
//    if (configState == NULL)
//        return 0;
//
//    i = 0;
//    FOREACH (configShard, configState->shards)
//    {
//        if (i == n)
//            return configShard->shardID;
//        
//        i++;
//    }
//    
//    return 0;
//}

/*
===============================================================================================

 Data commands

===============================================================================================
*/

int	SDBP_Get(ClientObj client_, uint64_t tableID, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Get(tableID, key);
}

int	SDBP_GetCStr(ClientObj client_, uint64_t tableID, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Get(tableID, key);
}

int	SDBP_Set(ClientObj client_, uint64_t tableID, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  value((char*) value_.c_str(), value_.length());

    return client->Set(tableID, key, value);
}

int	SDBP_SetCStr(ClientObj client_, uint64_t tableID, char* key_, int lenKey, char* value_, int lenValue)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  value;

    key.Wrap((char*) key_, lenKey);
    value.Wrap((char*) value_, lenValue);

    return client->Set(tableID, key, value);
}

int SDBP_Add(ClientObj client_, uint64_t tableID, const std::string& key_, int64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Add(tableID, key, number);
}

int SDBP_AddCStr(ClientObj client_, uint64_t tableID, char* key_, int len, int64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Add(tableID, key, number);
}

int SDBP_Delete(ClientObj client_, uint64_t tableID, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->Delete(tableID, key);
}

int SDBP_DeleteCStr(ClientObj client_, uint64_t tableID, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->Delete(tableID, key);
}

int SDBP_SequenceSet(ClientObj client_, uint64_t tableID, const std::string& key_, uint64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->SequenceSet(tableID, key, number);
}

int SDBP_SequenceSetCStr(ClientObj client_, uint64_t tableID, char* key_, int len, uint64_t number)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->SequenceSet(tableID, key, number);
}

int SDBP_SequenceNext(ClientObj client_, uint64_t tableID, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());

    return client->SequenceNext(tableID, key);
}

int SDBP_SequenceNextCStr(ClientObj client_, uint64_t tableID, char* key_, int len)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;

    key.Wrap((char*) key_, len);

    return client->SequenceNext(tableID, key);
}

int SDBP_ListKeys(ClientObj client_, uint64_t tableID, 
 const std::string& key_, const std::string& endKey_, const std::string& prefix_, 
 unsigned count, bool forwardDirection, bool skip)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  endKey((char*) endKey_.c_str(), endKey_.length());
    ReadBuffer  prefix((char*) prefix_.c_str(), prefix_.length());

    return client->ListKeys(tableID, key, endKey, prefix, count, forwardDirection, skip);
}

int SDBP_ListKeysCStr(ClientObj client_, uint64_t tableID, 
 char* key_, int keyLen, char* endKey_, int endKeyLen, char* prefix_, int prefixLen,
 unsigned count, bool forwardDirection, bool skip)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  endKey;
    ReadBuffer  prefix;

    key.Wrap((char*) key_, keyLen);
    endKey.Wrap((char*) endKey_, endKeyLen);
    prefix.Wrap((char*) prefix_, prefixLen);

    return client->ListKeys(tableID, key, endKey, prefix, count, forwardDirection, skip);
}

int SDBP_ListKeyValues(ClientObj client_, uint64_t tableID, 
 const std::string& key_, const std::string& endKey_, const std::string& prefix_,
 unsigned count, bool forwardDirection, bool skip)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  endKey((char*) endKey_.c_str(), endKey_.length());
    ReadBuffer  prefix((char*) prefix_.c_str(), prefix_.length());

    return client->ListKeyValues(tableID, key, endKey, prefix, count, forwardDirection, skip);
}

int SDBP_ListKeyValuesCStr(ClientObj client_, uint64_t tableID, 
 char* key_, int keyLen, char* endKey_, int endKeyLen, char* prefix_, int prefixLen,
 unsigned count, bool forwardDirection, bool skip)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  endKey;
    ReadBuffer  prefix;

    key.Wrap((char*) key_, keyLen);
    endKey.Wrap((char*) endKey_, endKeyLen);
    prefix.Wrap((char*) prefix_, prefixLen);

    return client->ListKeyValues(tableID, key, endKey, prefix, count, forwardDirection, skip);
}

int SDBP_Count(ClientObj client_, uint64_t tableID, 
 const std::string& key_, const std::string& endKey_, const std::string& prefix_,
 bool forwardDirection)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key((char*) key_.c_str(), key_.length());
    ReadBuffer  endKey((char*) endKey_.c_str(), endKey_.length());
    ReadBuffer  prefix((char*) prefix_.c_str(), prefix_.length());

    return client->Count(tableID, key, endKey, prefix, forwardDirection);
}

int SDBP_CountCStr(ClientObj client_, uint64_t tableID, 
 char* key_, int keyLen, char* endKey_, int endKeyLen, char* prefix_, int prefixLen,
 bool forwardDirection)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key;
    ReadBuffer  endKey;
    ReadBuffer  prefix;

    key.Wrap((char*) key_, keyLen);
    endKey.Wrap((char*) endKey_, endKeyLen);
    prefix.Wrap((char*) prefix_, prefixLen);

    return client->Count(tableID, key, endKey, prefix, forwardDirection);
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
        Log_SetDebug(true);
        Log_SetTimestamping(true);
        Log_SetThreadedOutput(true);
		Log_SetTarget(LOG_TARGET_STDERR);
	}
	else
	{
		Log_SetTrace(false);
        Log_SetDebug(false);
		Log_SetTarget(LOG_TARGET_NOWHERE);
	}
}

void SDBP_SetTraceBufferSize(unsigned traceBufferSize)
{
    Log_Set
}

void SDBP_SetLogFile(const std::string& filename)
{
    int     target;

    if (filename.length() > 0)
    {
        target = Log_GetTarget();
        target |= LOG_TARGET_FILE;
        Log_SetOutputFile(filename.c_str(), false);
        Log_SetTarget(target);
    }
    else
    {
        target = Log_GetTarget();
        target &= ~LOG_TARGET_FILE;
        Log_SetOutputFile("", false);
        Log_SetTarget(target);
    }
}

void SDBP_LogTrace(const std::string& msg)
{
    Log_Trace("Client app: %s", msg.c_str());
}

void SDBP_SetShardPoolSize(unsigned shardPoolSize)
{
    PooledShardConnection::SetPoolSize(shardPoolSize);
}

std::string SDBP_GetVersion()
{
    return "ScalienDB Client v" VERSION_STRING " " PLATFORM_STRING;
}

std::string SDBP_GetDebugString()
{
    return "Build date: " __DATE__ " " __TIME__ "\nBranch: " SOURCE_CONTROL_BRANCH "\nSource control version: " SOURCE_CONTROL_VERSION;
}
