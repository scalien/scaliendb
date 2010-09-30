#include "HTTPControllerSession.h"
#include "Controller.h"
#include "System/Config.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/Common/ContextTransport.h"
#include "Version.h"

void HTTPControllerSession::SetController(Controller* controller_)
{
    controller = controller_;
}

void HTTPControllerSession::SetConnection(HTTPConnection* conn_)
{
    session.SetConnection(conn_);
    conn_->SetOnClose(MFUNC(HTTPControllerSession, OnConnectionClose));
}

bool HTTPControllerSession::HandleRequest(HTTPRequest& request)
{
    ReadBuffer  cmd;
    UrlParam    params;
    
    session.ParseRequest(request, cmd, params);
    return ProcessCommand(cmd, params);
}

void HTTPControllerSession::OnComplete(ClientRequest* request, bool last)
{
    Buffer          tmp;
    ReadBuffer      rb;
    ClientResponse* response;

    response = &request->response;
    switch (response->type)
    {
    case CLIENTRESPONSE_OK:
        session.Print(ReadBuffer("OK"));
        break;
    case CLIENTRESPONSE_NUMBER:
        tmp.Writef("%U", response->number);
        rb.Wrap(tmp);
        session.Print(rb);
        break;
    case CLIENTRESPONSE_VALUE:
        session.Print(response->value);
        break;
    case CLIENTRESPONSE_GET_CONFIG_STATE:
        if (!last)
        {
            response->configState->Write(tmp, true);
            response->configState = NULL;
            rb.Wrap(tmp);
            session.Print(rb);
            controller->OnClientClose(this);
        }
        break;
    case CLIENTRESPONSE_NOSERVICE:
        session.Print(ReadBuffer("NOSERVICE"));
        break;
    case CLIENTRESPONSE_FAILED:
        session.Print(ReadBuffer("FAILED"));
        break;
    }
    
    if (last)
    {
        session.Flush();        
        delete request;
    }
}

bool HTTPControllerSession::IsActive()
{
    return true;
}

void HTTPControllerSession::PrintStatus()
{
    Buffer          buf;
    ConfigState*    configState;

    session.PrintPair("ScalienDB", "Controller");
    session.PrintPair("Version", VERSION_STRING);

    buf.Writef("%d", (int) controller->GetNodeID());
    buf.NullTerminate();
    session.PrintPair("NodeID", buf.GetBuffer());   

    buf.Writef("%d", (int) controller->GetMaster());
    buf.NullTerminate();
    session.PrintPair("Master", buf.GetBuffer());
    
    session.PrintPair("Controllers", configFile.GetValue("controllers", ""));
    
    session.Print("\n");
    session.Print("--- Configuration State ---\n");
    session.Print("\n");
    
    configState = controller->GetConfigState();
    PrintShardServers(configState);
    session.Print("\n");
    PrintQuorumMatrix(configState);
    session.Print("\n");
    PrintDatabases(configState);
    session.Print("\n");
    PrintShardMatrix(configState);
    
    session.Flush();
}

void HTTPControllerSession::PrintShardServers(ConfigState* configState)
{
    ConfigShardServer*      it;
    Buffer                  buffer;
    ReadBuffer              rb;
    uint64_t                ssID;
    
    if (configState->shardServers.GetLength() == 0)
    {
        session.Print("No shard servers configured\n");
    }
    else
    {
        session.Print("Shard servers:\n\n");
        ConfigState::ShardServerList& shardServers = configState->shardServers;
        for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
        {
            if (CONTEXT_TRANSPORT->IsConnected(it->nodeID))
                buffer.Writef("+ ");
            else
                buffer.Writef("- ");
            rb = it->endpoint.ToReadBuffer();
            ssID = it->nodeID - CONFIG_MIN_SHARD_NODE_ID;
            buffer.Appendf("ss%U (%R)\n", ssID, &rb);
            session.Print(buffer);
        }
    }
}

void HTTPControllerSession::PrintQuorumMatrix(ConfigState* configState)
{
    bool                    found;
    ConfigShardServer*      itShardServer;
    ConfigQuorum*           itQuorum;
    uint64_t*               itNodeID;
    Buffer                  buffer;
    uint64_t                ssID;
    
    if (configState->shardServers.GetLength() == 0 || configState->quorums.GetLength() == 0)
        return;
    
    session.Print("Quorum matrix:\n\n");
    ConfigState::ShardServerList& shardServers = configState->shardServers;
    ConfigState::QuorumList& quorums = configState->quorums;

    buffer.Writef("       ");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        ssID = itShardServer->nodeID - CONFIG_MIN_SHARD_NODE_ID;
        if (ssID < 10)
            buffer.Appendf("   ");
        else if (ssID < 100)
            buffer.Appendf("  ");
        else if (ssID < 1000)
            buffer.Appendf(" ");
        else
            buffer.Appendf("");
        buffer.Appendf("ss%U", ssID);
    }
    buffer.Appendf("\n");
    session.Print(buffer);

    buffer.Writef("      +");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        buffer.Appendf("------");
    }
    buffer.Appendf("\n");
    session.Print(buffer);
    
    for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = quorums.Next(itQuorum))
    {
        if (itQuorum->quorumID < 10)
            buffer.Writef("   ");
        else if (itQuorum->quorumID < 100)
            buffer.Writef("  ");
        else if (itQuorum->quorumID < 1000)
            buffer.Writef(" ");
        else
            buffer.Writef("");
        buffer.Appendf("q%U |", itQuorum->quorumID);
        ConfigQuorum::NodeList& activeNodes = itQuorum->activeNodes;
        for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
        {
            found = false;
            for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
            {
                if (itShardServer->nodeID == *itNodeID)
                {
                    found = true;
                    if (itQuorum->hasPrimary && itQuorum->primaryID == *itNodeID)
                        if (CONTEXT_TRANSPORT->IsConnected(*itNodeID))
                            buffer.Appendf("     P");
                        else
                            buffer.Appendf("     p");
                    else
                    {
                        if (CONTEXT_TRANSPORT->IsConnected(*itNodeID))
                            buffer.Appendf("     +");
                        else
                            buffer.Appendf("     -");
                    }
                    break;
                }
            }
            if (!found)
                buffer.Appendf("     .");

        }
        buffer.Appendf("\n");
        session.Print(buffer);
    }
}

void HTTPControllerSession::PrintDatabases(ConfigState* configState)
{
    ConfigDatabase*     itDatabase;
    uint64_t*           itTableID;
    uint64_t*           itShardID;
    ConfigTable*        table;
    ConfigShard*        shard;
    Buffer              buffer;
    
    session.Print("Databases, tables and shards:\n\n");
    
    ConfigState::DatabaseList& databases = configState->databases;
    for (itDatabase = databases.First(); itDatabase != NULL; itDatabase = databases.Next(itDatabase))
    {
        buffer.Writef("- %B(d%u)", &itDatabase->name, itDatabase->databaseID);
        List<uint64_t>& tables = itDatabase->tables;
        if (tables.GetLength() > 0)
            buffer.Appendf(":\n");
        else
            buffer.Appendf("\n");
        session.Print(buffer);
        for (itTableID = tables.First(); itTableID != NULL; itTableID = tables.Next(itTableID))
        {
            table = configState->GetTable(*itTableID);
            buffer.Writef("  - %B(t%U): [", table->tableID, &table->name);
            List<uint64_t>& shards = table->shards;
            for (itShardID = shards.First(); itShardID != NULL; itShardID = shards.Next(itShardID))
            {
                shard = configState->GetShard(*itShardID);
                buffer.Appendf("s%U => q%U", itShardID, shard->quorumID);
                if (itShardID != NULL)
                    buffer.Appendf(", ");
            }
            buffer.Appendf("]");
            session.Print(buffer);
        }
        session.Print("\n");
    }
}

void HTTPControllerSession::PrintShardMatrix(ConfigState* configState)
{
    bool                    found;
    ConfigShardServer*      itShardServer;
    ConfigShard*            itShard;
    ConfigQuorum*           quorum;
    uint64_t*               itNodeID;
    Buffer                  buffer;
    uint64_t                ssID;
    
    if (configState->shardServers.GetLength() == 0 ||
     configState->quorums.GetLength() == 0 ||
     configState->shards.GetLength() == 0)
        return;
    
    session.Print("Shard matrix:\n\n");
    ConfigState::ShardServerList& shardServers = configState->shardServers;
    ConfigState::ShardList& shards = configState->shards;
    
    buffer.Writef("       ");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        ssID = itShardServer->nodeID - CONFIG_MIN_SHARD_NODE_ID;
        if (ssID < 10)
            buffer.Appendf("   ");
        else if (ssID < 100)
            buffer.Appendf("  ");
        else if (ssID < 1000)
            buffer.Appendf(" ");
        else
            buffer.Appendf("");
        buffer.Appendf("ss%U", ssID);
    }
    buffer.Appendf("\n");
    session.Print(buffer);

    buffer.Writef("      +");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        buffer.Appendf("------");
    }
    buffer.Appendf("\n");
    session.Print(buffer);
    
    for (itShard = shards.First(); itShard != NULL; itShard = shards.Next(itShard))
    {
        if (itShard->shardID < 10)
            buffer.Writef("   ");
        else if (itShard->shardID < 100)
            buffer.Writef("  ");
        else if (itShard->shardID < 1000)
            buffer.Writef(" ");
        else
            buffer.Writef("");
        buffer.Appendf("s%U |", itShard->shardID);
        quorum = configState->GetQuorum(itShard->quorumID);
        ConfigQuorum::NodeList& activeNodes = quorum->activeNodes;
        for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
        {
            found = false;
            for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
            {
                if (itShardServer->nodeID == *itNodeID)
                {
                    found = true;
                    if (quorum->hasPrimary && quorum->primaryID == *itNodeID)
                        if (CONTEXT_TRANSPORT->IsConnected(*itNodeID))
                            buffer.Appendf("     P");
                        else
                            buffer.Appendf("     p");
                    else
                    {
                        if (CONTEXT_TRANSPORT->IsConnected(*itNodeID))
                            buffer.Appendf("     +");
                        else
                            buffer.Appendf("     -");
                    }
                    break;
                }
            }
            if (!found)
                buffer.Appendf("     .");

        }
        buffer.Appendf("\n");
        session.Print(buffer);
    }
}

bool HTTPControllerSession::ProcessCommand(ReadBuffer& cmd, UrlParam& params)
{
    ClientRequest*  request;
    
    if (HTTP_MATCH_COMMAND(cmd, ""))
    {
        PrintStatus();
        return true;
    }

    request = ProcessControllerCommand(cmd, params);
    if (!request)
        return false;

    request->session = this;
    controller->OnClientRequest(request);
    
    return true;
}

ClientRequest* HTTPControllerSession::ProcessControllerCommand(ReadBuffer& cmd, UrlParam& params)
{
    if (HTTP_MATCH_COMMAND(cmd, "getmaster"))
        return ProcessGetMaster(params);
    if (HTTP_MATCH_COMMAND(cmd, "getstate"))
        return ProcessGetState(params);
    if (HTTP_MATCH_COMMAND(cmd, "createquorum"))
        return ProcessCreateQuorum(params);
//  if (HTTP_MATCH_COMMAND(cmd, "increasequorum"))
//      return ProcessIncreaseQuorum(params);
//  if (HTTP_MATCH_COMMAND(cmd, "decreasequorum"))
//      return ProcessDecreaseQuorum(params);
    if (HTTP_MATCH_COMMAND(cmd, "createdatabase"))
        return ProcessCreateDatabase(params);
    if (HTTP_MATCH_COMMAND(cmd, "renamedatabase"))
        return ProcessRenameDatabase(params);
    if (HTTP_MATCH_COMMAND(cmd, "deletedatabase"))
        return ProcessDeleteDatabase(params);
    if (HTTP_MATCH_COMMAND(cmd, "createtable"))
        return ProcessCreateTable(params);
    if (HTTP_MATCH_COMMAND(cmd, "renametable"))
        return ProcessRenameTable(params);
    if (HTTP_MATCH_COMMAND(cmd, "deletetable"))
        return ProcessDeleteTable(params);
    
    return NULL;
}

ClientRequest* HTTPControllerSession::ProcessGetMaster(UrlParam& /*params*/)
{
    ClientRequest*  request;
    
    request = new ClientRequest;
    request->GetMaster(0);
    
    return request;
}

ClientRequest* HTTPControllerSession::ProcessGetState(UrlParam& /*params*/)
{
    ClientRequest*  request;
    
    request = new ClientRequest;
    request->GetConfigState(0);
    
    return request;
}

ClientRequest* HTTPControllerSession::ProcessCreateQuorum(UrlParam& params)
{
    typedef ClientRequest::NodeList NodeList;
    
    ClientRequest*  request;
    char            productionType;
    NodeList        nodes;
    ReadBuffer      tmp;
    char*           next;
    unsigned        nread;
    uint64_t        nodeID;
    
    HTTP_GET_PARAM(params, "productionType", tmp);
    if (tmp.GetLength() > 1)
        return NULL;

    // TODO: validate values for productionType
    productionType = tmp.GetCharAt(0);
    
    // parse comma separated nodeID values
    HTTP_GET_PARAM(params, "nodes", tmp);
    while ((next = FindInBuffer(tmp.GetBuffer(), tmp.GetLength(), ',')) != NULL)
    {
        nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
        if (nread != next - tmp.GetBuffer())
            return NULL;
        next++;
        tmp.Advance(next - tmp.GetBuffer());
        nodes.Append(nodeID);
    }
    
    nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
    if (nread != tmp.GetLength())
        return NULL;
    nodes.Append(nodeID);

    request = new ClientRequest;
    request->CreateQuorum(0, productionType, nodes);
    
    return request;
}

//ClientRequest* HTTPControllerSession::ProcessIncreaseQuorum(UrlParam& params)
//{
//  ClientRequest*  request;
//  uint64_t        shardID;
//  uint64_t        nodeID;
//  
//  HTTP_GET_U64_PARAM(params, "shardID", shardID);
//  HTTP_GET_U64_PARAM(params, "nodeID", nodeID);
//
//  request = new ClientRequest;
//  request->IncreaseQuorum(0, shardID, nodeID);
//
//  return request;
//}
//
//ClientRequest* HTTPControllerSession::ProcessDecreaseQuorum(UrlParam& params)
//{
//  ClientRequest*  request;
//  uint64_t        shardID;
//  uint64_t        nodeID;
//  
//  HTTP_GET_U64_PARAM(params, "shardID", shardID);
//  HTTP_GET_U64_PARAM(params, "nodeID", nodeID);
//
//  request = new ClientRequest;
//  request->DecreaseQuorum(0, shardID, nodeID);
//
//  return request;
//}

ClientRequest* HTTPControllerSession::ProcessCreateDatabase(UrlParam& params)
{
    ClientRequest*  request;
    char            productionType;
    ReadBuffer      name;
    ReadBuffer      tmp;
    
    HTTP_GET_PARAM(params, "productionType", tmp);
    if (tmp.GetLength() > 1)
        return NULL;

    // TODO: validate values for productionType
    productionType = tmp.GetCharAt(0);

    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->CreateDatabase(0, productionType, name);

    return request;
}

ClientRequest* HTTPControllerSession::ProcessRenameDatabase(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    ReadBuffer      name;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->RenameDatabase(0, databaseID, name);

    return request;
}

ClientRequest* HTTPControllerSession::ProcessDeleteDatabase(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);

    request = new ClientRequest;
    request->DeleteDatabase(0, databaseID);

    return request;
}

ClientRequest* HTTPControllerSession::ProcessCreateTable(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        quorumID;
    ReadBuffer      name;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->CreateTable(0, databaseID, quorumID, name);

    return request;
}

ClientRequest* HTTPControllerSession::ProcessRenameTable(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      name;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->RenameTable(0, databaseID, tableID, name);

    return request;
}

ClientRequest* HTTPControllerSession::ProcessDeleteTable(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);

    request = new ClientRequest;
    request->DeleteTable(0, databaseID, tableID);

    return request;
}

void HTTPControllerSession::OnConnectionClose()
{
    controller->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}
