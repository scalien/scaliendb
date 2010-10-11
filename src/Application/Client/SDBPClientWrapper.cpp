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
	nodes[num++] = strdup(node.c_str());
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
    
    return result->TransportStatus();
}

int SDBP_ResultCommandStatus(ResultObj result_)
{
    Result*     result = (Result*) result_;
    
    if (!result)
        return SDBP_API_ERROR;
    
    return result->CommandStatus();
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

uint64_t SDBP_GetDatabaseID(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name = name_.c_str();
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
    ReadBuffer  name = name_.c_str();
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
    ReadBuffer  name = name_.c_str();

    return client->UseDatabase(name);
}

int SDBP_UseTable(ClientObj client_, const std::string& name_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  name = name_.c_str();

    return client->UseTable(name);
}

int	SDBP_Get(ClientObj client_, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();

    return client->Get(key);
}

int	SDBP_Set(ClientObj client_, const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();
    ReadBuffer  value = value_.c_str();

    return client->Set(key, value);
}

int SDBP_Delete(ClientObj client_, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();

    return client->Delete(key);
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
    Log_SetTrace(trace);
}
