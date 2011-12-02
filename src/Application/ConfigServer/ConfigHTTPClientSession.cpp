#include "ConfigHTTPClientSession.h"
#include "ConfigServer.h"
#include "JSONConfigState.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/ContextTransport.h"
#include "Version.h"
#include "ConfigHeartbeatManager.h"

#define PARAM_BOOL_VALUE(param)                             \
    ((ReadBuffer::Cmp((param), "yes") == 0 ||               \
    ReadBuffer::Cmp((param), "true") == 0 ||                \
    ReadBuffer::Cmp((param), "on") == 0 ||                  \
    ReadBuffer::Cmp((param), "1") == 0) ? true : false)

#define MAKE_PRINTABLE(a) if (!a.IsAsciiPrintable()) { a.ToHexadecimal(); }

void ConfigHTTPClientSession::SetConfigServer(ConfigServer* configServer_)
{
    configServer = configServer_;
}

void ConfigHTTPClientSession::SetConnection(HTTPConnection* conn)
{
    ReadBuffer  origin;
    
    session.SetConnection(conn);
    conn->SetOnClose(MFUNC(ConfigHTTPClientSession, OnConnectionClose));
    origin.Wrap("*");
    conn->SetOrigin(origin);
}

bool ConfigHTTPClientSession::HandleRequest(HTTPRequest& request)
{
    ReadBuffer  cmd;
    
    session.ParseRequest(request, cmd, params);
    return ProcessCommand(cmd);
}

void ConfigHTTPClientSession::OnComplete(ClientRequest* request, bool last)
{
    Buffer          tmp;
    ReadBuffer      rb;
    ClientResponse* response;
    JSONConfigState jsonConfigState;

    numResponses++;

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
    case CLIENTRESPONSE_CONFIG_STATE:
        {
            // TODO: HACK
            if (session.IsFlushed())
                return;
            session.json.Start();
            jsonConfigState.SetHeartbeats(&configServer->GetHeartbeatManager()->GetHeartbeats());
            jsonConfigState.SetConfigState(response->configState.Get());
            jsonConfigState.SetJSONBufferWriter(&session.json.GetBufferWriter());
            jsonConfigState.Write();
            session.Flush();
            return;
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

bool ConfigHTTPClientSession::IsActive()
{
    return true;
}

void ConfigHTTPClientSession::PrintStatus()
{
    ReadBuffer      rb;
    Buffer          buf;
    ConfigState*    configState;
    char            hexbuf[64 + 1];
    unsigned        num;
    uint64_t        nodeID;
    uint64_t        elapsed;

    session.PrintPair("ScalienDB", "Controller");
    session.PrintPair("Version", VERSION_STRING);

    buf.Writef("%U", GetProcessID());
    buf.NullTerminate();
    session.PrintPair("ProcessID", buf.GetBuffer());

    UInt64ToBufferWithBase(hexbuf, sizeof(hexbuf), REPLICATION_CONFIG->GetClusterID(), 64);
    session.PrintPair("ClusterID", hexbuf);

    buf.Writef("%d", (int) configServer->GetNodeID());
    buf.NullTerminate();
    session.PrintPair("NodeID", buf.GetBuffer());   

    buf.Writef("%d", (int) configServer->GetQuorumProcessor()->GetMaster());
    buf.NullTerminate();
    session.PrintPair("Master", buf.GetBuffer());

    buf.Writef("%d", (int) configServer->GetQuorumProcessor()->GetPaxosID());
    buf.NullTerminate();
    session.PrintPair("Round", buf.GetBuffer());

    elapsed = (EventLoop::Now() - configServer->GetQuorumProcessor()->GetLastLearnChosenTime()) / 1000.0;
    buf.Writef("%U", elapsed);
    buf.NullTerminate();
    session.PrintPair("Seconds since last replication", buf.GetBuffer());

    buf.Writef("%u", configServer->GetNumSDBPClients());
    buf.NullTerminate();
    session.PrintPair("Number of clients", buf.GetBuffer());  
    
    session.Print("\nControllers:\n");
    num = configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < num; nodeID++)
    {
        if (CONTEXT_TRANSPORT->IsConnected(nodeID))
            buf.Writef("+ ");
        else
            buf.Writef("- ");
        rb = configFile.GetListValue("controllers", nodeID, "");
        buf.Appendf("c%U (%R)", nodeID, &rb);
        session.Print(buf);
    }
    session.Print("");

    configState = configServer->GetDatabaseManager()->GetConfigState();
    PrintShardServers(configState);
    session.Print("");
    PrintShards(configState);
    session.Print("");
    PrintQuorumMatrix(configState);
    session.Print("");
    PrintDatabases(configState);
    session.Print("");
    PrintShardMatrix(configState);
    
    session.Flush();
}

void ConfigHTTPClientSession::PrintShardServers(ConfigState* configState)
{
    ConfigShardServer*      it;
    Buffer                  buffer;
    ReadBuffer              rb;
    uint64_t                ssID;
    
    if (configState->shardServers.GetLength() == 0)
    {
        session.Print("No shard servers configured...");
    }
    else
    {
        session.Print("Shard servers:\n");
        ConfigState::ShardServerList& shardServers = configState->shardServers;
        for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
        {
            if (configServer->GetHeartbeatManager()->HasHeartbeat(it->nodeID))
            {
                if (CONTEXT_TRANSPORT->IsConnected(it->nodeID))
                    buffer.Writef("+ ");
                else
                    buffer.Writef("! ");
            }
            else
            {
                if (CONTEXT_TRANSPORT->IsConnected(it->nodeID))
                    buffer.Writef("* ");
                else
                    buffer.Writef("- ");
            }
            rb = it->endpoint.ToReadBuffer();
            ssID = it->nodeID; // - CONFIG_MIN_SHARD_NODE_ID;
            buffer.Appendf("ss%U (%R)", ssID, &rb);
            session.Print(buffer);
        }
    }
}

void ConfigHTTPClientSession::PrintShards(ConfigState* configState)
{
    ConfigShard*    it;
    Buffer          buffer;
    Buffer          firstKey, lastKey, splitKey;
    
    if (configState->shards.GetLength() == 0)
    {
        session.Print("No shards...");
    }
    else
    {
        session.Print("Shards:\n");
        ConfigState::ShardList& shards = configState->shards;
        for (it = shards.First(); it != NULL; it = shards.Next(it))
        {
            buffer.Clear();
            if (it->state == CONFIG_SHARD_STATE_SPLIT_CREATING)
                buffer.Appendf("* ");
            else
                buffer.Appendf("- ");

            firstKey.Write(it->firstKey);
            lastKey.Write(it->lastKey);
            splitKey.Write(it->splitKey);
            MAKE_PRINTABLE(firstKey);
            MAKE_PRINTABLE(lastKey);
            MAKE_PRINTABLE(splitKey);

            buffer.Appendf("s%U: range [%B, %B], size: %s (isSplitable: %b, split key: %B)",
             it->shardID, &firstKey, &lastKey,
             HUMAN_BYTES(it->shardSize), it->isSplitable, &splitKey);

            session.Print(buffer);
        }
    }
}

void ConfigHTTPClientSession::PrintQuorumMatrix(ConfigState* configState)
{
    bool                    found;
    ConfigShardServer*      itShardServer;
    ConfigQuorum*           itQuorum;
    Buffer                  buffer;
    uint64_t                ssID;
    unsigned                len;
    
    if (configState->shardServers.GetLength() == 0 || configState->quorums.GetLength() == 0)
        return;
    
    session.Print("Quorum matrix:\n");
    ConfigState::ShardServerList& shardServers = configState->shardServers;
    ConfigState::QuorumList& quorums = configState->quorums;

    len = 0;
    for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = quorums.Next(itQuorum))
    {
        if (itQuorum->name.GetLength() > len)
            len = itQuorum->name.GetLength();
    }

    buffer.Append(' ', len + 3);

    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        ssID = itShardServer->nodeID; // - CONFIG_MIN_SHARD_NODE_ID;
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
    session.Print(buffer);

    buffer.Write(' ', len + 2);
    buffer.Appendf("+");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        buffer.Appendf("------");
    }
    session.Print(buffer);
    
    for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = quorums.Next(itQuorum))
    {
        buffer.Write(' ', len - itQuorum->name.GetLength());
        buffer.Appendf(" %B |", &itQuorum->name);
        for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
        {
            found = false;

            if (itQuorum->IsActiveMember(itShardServer->nodeID))
            {
                found = true;
                if (itQuorum->hasPrimary && itQuorum->primaryID == itShardServer->nodeID)
                    if (configServer->GetHeartbeatManager()->HasHeartbeat(itShardServer->nodeID) &&
                     CONTEXT_TRANSPORT->IsConnected(itShardServer->nodeID))
                        buffer.Appendf("     P");
                    else
                        buffer.Appendf("     !");
                else
                {
                    if (configServer->GetHeartbeatManager()->HasHeartbeat(itShardServer->nodeID))
                        buffer.Appendf("     +");
                    else
                        buffer.Appendf("     -");
                }
            }

            if (itQuorum->IsInactiveMember(itShardServer->nodeID))
            {
                found = true;
                buffer.Appendf("     i");
            }

            if (!found)
                buffer.Appendf("      ");

        }
        session.Print(buffer);
    }
}

void ConfigHTTPClientSession::PrintDatabases(ConfigState* configState)
{
    ConfigDatabase*     itDatabase;
    uint64_t*           itTableID;
    uint64_t*           itShardID;
    ConfigTable*        table;
    ConfigShard*        shard;
    Buffer              buffer;
    
    session.Print("Databases, tables and shards:\n");
    
    FOREACH (itDatabase, configState->databases)
    {
        buffer.Writef("- %B(d%u)", &itDatabase->name, itDatabase->databaseID);
        List<uint64_t>& tables = itDatabase->tables;
        if (tables.GetLength() > 0)
            buffer.Appendf(":");
        session.Print(buffer);
        for (itTableID = tables.First(); itTableID != NULL; itTableID = tables.Next(itTableID))
        {
            table = configState->GetTable(*itTableID);
            buffer.Writef("  - %B(t%U): [", &table->name, table->tableID);
            List<uint64_t>& shards = table->shards;
            for (itShardID = shards.First(); itShardID != NULL; itShardID = shards.Next(itShardID))
            {
                shard = configState->GetShard(*itShardID);
                buffer.Appendf("s%U => %B", *itShardID, &configState->GetQuorum(shard->quorumID)->name);
                if (shards.Next(itShardID) != NULL)
                    buffer.Appendf(", ");
            }
            buffer.Appendf("]");
            session.Print(buffer);
        }
        session.Print("");
    }
}

void ConfigHTTPClientSession::PrintShardMatrix(ConfigState* configState)
{
    bool                    found;
    ConfigShardServer*      itShardServer;
    ConfigShard*            itShard;
    ConfigQuorum*           quorum;
    Buffer                  buffer;
    uint64_t                ssID;
    
    if (configState->shardServers.GetLength() == 0 ||
     configState->quorums.GetLength() == 0 ||
     configState->shards.GetLength() == 0)
        return;
    
    session.Print("Shard matrix:\n");
    ConfigState::ShardServerList& shardServers = configState->shardServers;
    ConfigState::ShardList& shards = configState->shards;
    
    buffer.Writef("       ");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        ssID = itShardServer->nodeID; // - CONFIG_MIN_SHARD_NODE_ID;
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
    session.Print(buffer);

    buffer.Writef("      +");
    for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
    {
        buffer.Appendf("------");
    }
    session.Print(buffer);
    
    FOREACH (itShard, shards)
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
        for (itShardServer = shardServers.First(); itShardServer != NULL; itShardServer = shardServers.Next(itShardServer))
        {
            found = false;
            
            if (quorum->IsActiveMember(itShardServer->nodeID))
            {
                found = true;
                if (quorum->hasPrimary && quorum->primaryID == itShardServer->nodeID)
                    if (CONTEXT_TRANSPORT->IsConnected(itShardServer->nodeID))
                        buffer.Appendf("     P");
                    else
                        buffer.Appendf("     !");
                else
                {
                    if (CONTEXT_TRANSPORT->IsConnected(itShardServer->nodeID))
                        buffer.Appendf("     +");
                    else
                        buffer.Appendf("     -");
                }
            }
            
            if (!found)
                buffer.Appendf("      ");

        }
        session.Print(buffer);
    }
}

void ConfigHTTPClientSession::PrintConfigState()
{
    JSONConfigState     jsonConfigState;
    
    jsonConfigState.SetHeartbeats(&configServer->GetHeartbeatManager()->GetHeartbeats());
    jsonConfigState.SetConfigState(configServer->GetDatabaseManager()->GetConfigState());
    jsonConfigState.SetJSONBufferWriter(&session.json.GetBufferWriter());

    session.json.Start();
    jsonConfigState.Write();
    session.Flush();
}

void ConfigHTTPClientSession::ProcessActivateShardServer()
{
    bool        force;
    unsigned    nread;
    uint64_t    nodeID;
    ReadBuffer  param;
    Buffer      wb;

    if (!params.GetNamed("nodeID", sizeof("nodeID") - 1, param))
        goto Failed;
    nodeID = BufferToUInt64(param.GetBuffer(), param.GetLength(), &nread);
    if (nread != param.GetLength())
        goto Failed;

    force = false;
    if (HTTP_GET_OPT_PARAM(params, "force", param))
        force = PARAM_BOOL_VALUE(param);

    configServer->GetActivationManager()->TryActivateShardServer(nodeID, true, force);

    
    session.Print("Activation process started...");
    goto End;
    
Failed:
    session.Print("FAILED. Specify a nodeID!");

End:
    session.Flush();
    return;    
}

void ConfigHTTPClientSession::ProcessSettings()
{
    ReadBuffer  param;
    bool        boolValue;
    uint64_t    logStatTime;
    uint64_t    shardSplitSize;
    uint64_t    traceBufferSize;
    
    if (HTTP_GET_OPT_PARAM(params, "trace", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetTrace(boolValue);
        Log_Flush();
        session.PrintPair("Trace", boolValue ? "on" : "off");
    }

    if (HTTP_GET_OPT_PARAM(params, "traceBufferSize", param))
    {
        // initialize variable, because conversion may fail
        traceBufferSize = 0;
        HTTP_GET_OPT_U64_PARAM(params, "traceBufferSize", traceBufferSize);
        // we expect traceBufferSize is in bytes
        Log_SetTraceBufferSize((unsigned) traceBufferSize);
        session.PrintPair("TraceBufferSize", INLINE_PRINTF("%u", 100, (unsigned) traceBufferSize));
    }

    if (HTTP_GET_OPT_PARAM(params, "debug", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetDebug(boolValue);
        Log_Flush();
        session.PrintPair("Debug", boolValue ? "on" : "off");
    }

    if (HTTP_GET_OPT_PARAM(params, "logStatTime", param))
    {
        // initialize variable, because conversion may fail
        logStatTime = 0;
        HTTP_GET_OPT_U64_PARAM(params, "logStatTime", logStatTime);
        // we expect logStatTime is in seconds
        configServer->SetLogStatTimeout(logStatTime * 1000);
        session.PrintPair("LogStatTime", INLINE_PRINTF("%u", 100, (unsigned) logStatTime));
    }

    if (HTTP_GET_OPT_PARAM(params, "shardSplitSize", param))
    {
        // initialize variable, because conversion may fail
        shardSplitSize = 0;
        HTTP_GET_OPT_U64_PARAM(params, "shardSplitSize", shardSplitSize);
        // we expect shardSplitSize is in MegaBytes 
        configServer->GetHeartbeatManager()->SetShardSplitSize(shardSplitSize * 1000 * 1000);
        session.PrintPair("ShardSplitSize", INLINE_PRINTF("%u", 100, (unsigned) shardSplitSize));
    }

    session.Flush();
}

void ConfigHTTPClientSession::PrintConfigFile()
{
    ConfigVar*  configVar;
    Buffer      value;

    FOREACH (configVar, configFile)
    {
        // fix terminating zeroes
        value.Write(configVar->value);
        value.Shorten(1);
        // replace zeroes back to commas
        for (unsigned i = 0; i < value.GetLength(); i++)
        {
            if (value.GetCharAt(i) == '\0')
                value.SetCharAt(i, ',');
        }
        session.PrintPair(configVar->name, value);
    }

    session.Flush();
}

bool ConfigHTTPClientSession::ProcessCommand(ReadBuffer& cmd)
{
    ClientRequest*  request;
    
    if (HTTP_MATCH_COMMAND(cmd, ""))
    {
        PrintStatus();
        return true;
    }
    if (HTTP_MATCH_COMMAND(cmd, "getConfigState"))
    {
        PrintConfigState();
        return true;
    }
    if (HTTP_MATCH_COMMAND(cmd, "activateShardserver"))
    {
        ProcessActivateShardServer();
        return true;
    }
    if (HTTP_MATCH_COMMAND(cmd, "settings"))
    {
        ProcessSettings();
        return true;
    }
    if (HTTP_MATCH_COMMAND(cmd, "config"))
    {
        PrintConfigFile();
        return true;
    }
    if (HTTP_MATCH_COMMAND(cmd, "debug"))
    {
        ProcessDebugCommand();
        return true;
    }

    request = ProcessConfigCommand(cmd);
    if (!request)
        return false;

    request->session = this;
    configServer->OnClientRequest(request);
    
    return true;
}

void ConfigHTTPClientSession::ProcessDebugCommand()
{
#ifdef DEBUG
    ReadBuffer  param;
    bool        boolValue;

    if (HTTP_GET_OPT_PARAM(params, "crash", param))
    {
        Log_Debug("Crashing server from web console...");
        char* null = NULL;
        *null = 0;
        // Access violation
    }

    if (HTTP_GET_OPT_PARAM(params, "sleep", param))
    {
        unsigned sleep = 0;
        param.Readf("%u", &sleep);
        Log_Debug("Sleeping for %u seconds", sleep);
        session.Print("Start sleeping");
        MSleep(sleep * 1000);
        session.Print("Sleep finished");
    }

    if (HTTP_GET_OPT_PARAM(params, "log", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetDebug(boolValue);
        session.PrintPair("Debug", boolValue ? "on" : "off");
    }

    session.Flush();
#endif
}

ClientRequest* ConfigHTTPClientSession::ProcessConfigCommand(ReadBuffer& cmd)
{
    if (HTTP_MATCH_COMMAND(cmd, "getMaster"))
        return ProcessGetMaster();
    if (HTTP_MATCH_COMMAND(cmd, "getMasterHttp"))
        return ProcessGetMasterHTTP();
    if (HTTP_MATCH_COMMAND(cmd, "getState"))
        return ProcessGetState();
    if (HTTP_MATCH_COMMAND(cmd, "pollConfigState"))
        return ProcessPollConfigState();
    if (HTTP_MATCH_COMMAND(cmd, "unregisterShardserver"))
        return ProcessUnregisterShardServer();
    if (HTTP_MATCH_COMMAND(cmd, "createQuorum"))
        return ProcessCreateQuorum();
    if (HTTP_MATCH_COMMAND(cmd, "renameQuorum"))
        return ProcessRenameQuorum();
    if (HTTP_MATCH_COMMAND(cmd, "deleteQuorum"))
        return ProcessDeleteQuorum();
    if (HTTP_MATCH_COMMAND(cmd, "addShardserverToQuorum"))
        return ProcessAddShardServerToQuorum();
    if (HTTP_MATCH_COMMAND(cmd, "removeShardserverFromQuorum"))
        return ProcessRemoveShardServerFromQuorum();
    if (HTTP_MATCH_COMMAND(cmd, "setPriority"))
        return ProcessSetPriority();
    if (HTTP_MATCH_COMMAND(cmd, "createDatabase"))
        return ProcessCreateDatabase();
    if (HTTP_MATCH_COMMAND(cmd, "renameDatabase"))
        return ProcessRenameDatabase();
    if (HTTP_MATCH_COMMAND(cmd, "deleteDatabase"))
        return ProcessDeleteDatabase();
    if (HTTP_MATCH_COMMAND(cmd, "createTable"))
        return ProcessCreateTable();
    if (HTTP_MATCH_COMMAND(cmd, "renameTable"))
        return ProcessRenameTable();
    if (HTTP_MATCH_COMMAND(cmd, "deleteTable"))
        return ProcessDeleteTable();
    if (HTTP_MATCH_COMMAND(cmd, "truncateTable"))
        return ProcessTruncateTable();    
    if (HTTP_MATCH_COMMAND(cmd, "freezeTable"))
        return ProcessFreezeTable();    
    if (HTTP_MATCH_COMMAND(cmd, "unfreezeTable"))
        return ProcessUnfreezeTable();    
    if (HTTP_MATCH_COMMAND(cmd, "splitShard"))
        return ProcessSplitShard();
    if (HTTP_MATCH_COMMAND(cmd, "migrateShard"))
        return ProcessMigrateShard();
    
    return NULL;
}

ClientRequest* ConfigHTTPClientSession::ProcessGetMaster()
{
    ClientRequest*  request;
    
    request = new ClientRequest;
    request->GetMaster(0);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessGetMasterHTTP()
{
    ClientRequest*  request;
    
    request = new ClientRequest;
    request->GetMasterHTTP(0);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessGetState()
{
    ClientRequest*  request;
    
    request = new ClientRequest;
    request->GetConfigState(0);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessPollConfigState()
{
    ClientRequest*  request;
    
    numResponses = 0;
    
    request = new ClientRequest;
    request->GetConfigState(0, 60*1000);    // default changeTimeout is 60 sec
    request->lastChangeTime = NowClock();

    HTTP_GET_OPT_U64_PARAM(params, "paxosID", request->paxosID);
    HTTP_GET_OPT_U64_PARAM(params, "changeTimeout", request->changeTimeout);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessUnregisterShardServer()
{
    ClientRequest*  request;
    uint64_t        nodeID;
    
    HTTP_GET_U64_PARAM(params, "nodeID", nodeID);

    request = new ClientRequest;
    request->UnregisterShardServer(0, nodeID);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessCreateQuorum()
{
    ClientRequest*  request;
    List<uint64_t>  nodes;
    ReadBuffer      name;
    ReadBuffer      tmp;
    char*           next;
    unsigned        nread;
    uint64_t        nodeID;
    
    // parse comma separated nodeID values
    HTTP_GET_OPT_PARAM(params,  "name",  name);
    HTTP_GET_PARAM(params,      "nodes", tmp);
    while ((next = FindInBuffer(tmp.GetBuffer(), tmp.GetLength(), ',')) != NULL)
    {
        nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
        if (nread != (unsigned) (next - tmp.GetBuffer()))
            return NULL;
        next++;
        tmp.Advance((unsigned) (next - tmp.GetBuffer()));
        nodes.Append(nodeID);
    }
    
    nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
    if (nread != tmp.GetLength())
        return NULL;
    nodes.Append(nodeID);

    request = new ClientRequest;
    request->CreateQuorum(0, name, nodes);
    
    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessRenameQuorum()
{
    ClientRequest*  request;
    uint64_t        quorumID;
    ReadBuffer      name;

    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->RenameQuorum(0, quorumID, name);

    return request;    
}

ClientRequest* ConfigHTTPClientSession::ProcessDeleteQuorum()
{
    ClientRequest*  request;
    uint64_t        quorumID;

    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);

    request = new ClientRequest;
    request->DeleteQuorum(0, quorumID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessAddShardServerToQuorum()
{
    ClientRequest*  request;
    uint64_t        quorumID;
    uint64_t        nodeID;

    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
    HTTP_GET_U64_PARAM(params, "nodeID", nodeID);

    request = new ClientRequest;
    request->AddShardServerToQuorum(0, quorumID, nodeID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessRemoveShardServerFromQuorum()
{
    ClientRequest*  request;
    uint64_t        quorumID;
    uint64_t        nodeID;

    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
    HTTP_GET_U64_PARAM(params, "nodeID", nodeID);

    request = new ClientRequest;
    request->RemoveShardServerFromQuorum(0, quorumID, nodeID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessSetPriority()
{
    ClientRequest*  request;
    uint64_t        quorumID;
    uint64_t        nodeID;
    uint64_t        priority;

    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
    HTTP_GET_U64_PARAM(params, "nodeID", nodeID);
    HTTP_GET_U64_PARAM(params, "priority", priority);

    request = new ClientRequest;
    request->SetPriority(0, quorumID, nodeID, priority);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessCreateDatabase()
{
    ClientRequest*  request;
    ReadBuffer      name;
    ReadBuffer      tmp;
    
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->CreateDatabase(0, name);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessRenameDatabase()
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

ClientRequest* ConfigHTTPClientSession::ProcessDeleteDatabase()
{
    ClientRequest*  request;
    uint64_t        databaseID;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);

    request = new ClientRequest;
    request->DeleteDatabase(0, databaseID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessCreateTable()
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

ClientRequest* ConfigHTTPClientSession::ProcessRenameTable()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      name;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "name", name);

    request = new ClientRequest;
    request->RenameTable(0, tableID, name);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessDeleteTable()
{
    ClientRequest*  request;
    uint64_t        tableID;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);

    request = new ClientRequest;
    request->DeleteTable(0, tableID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessTruncateTable()
{
    ClientRequest*  request;
    uint64_t        tableID;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);

    request = new ClientRequest;
    request->TruncateTable(0, tableID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessFreezeTable()
{
    ClientRequest*  request;
    uint64_t        tableID;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);

    request = new ClientRequest;
    request->FreezeTable(0, tableID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessUnfreezeTable()
{
    ClientRequest*  request;
    uint64_t        tableID;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);

    request = new ClientRequest;
    request->UnfreezeTable(0, tableID);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessSplitShard()
{
    ClientRequest*  request;
    uint64_t        shardID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "shardID", shardID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->SplitShard(0, shardID, key);

    return request;
}

ClientRequest* ConfigHTTPClientSession::ProcessMigrateShard()
{
    ClientRequest*  request;
    uint64_t        quorumID;
    uint64_t        shardID;
    
    HTTP_GET_U64_PARAM(params, "shardID", shardID);
    HTTP_GET_U64_PARAM(params, "quorumID", quorumID);

    request = new ClientRequest;
    request->MigrateShard(0, shardID, quorumID);

    return request;
}

void ConfigHTTPClientSession::OnConnectionClose()
{
    configServer->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}
