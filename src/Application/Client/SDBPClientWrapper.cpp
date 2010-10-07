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

int	SDBP_Get(ClientObj client_, uint64_t databaseID, uint64_t tableID, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();

    return client->Get(databaseID, tableID, key);
}

int	SDBP_Set(ClientObj client_, uint64_t databaseID, uint64_t tableID, 
 const std::string& key_, const std::string& value_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();
    ReadBuffer  value = value_.c_str();

    return client->Set(databaseID, tableID, key, value);
}

int SDBP_Delete(ClientObj client_, uint64_t databaseID, uint64_t tableID, const std::string& key_)
{
    Client*     client = (Client*) client_;
    ReadBuffer  key = key_.c_str();

    return client->Delete(databaseID, tableID, key);
}
