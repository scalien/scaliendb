#include "ShardHTTPClientSession.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "System/FileSystem.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StoragePageCache.h"
#include "Version.h"

#define PARAM_BOOL_VALUE(param)                         \
    ((ReadBuffer::Cmp((param), "yes") == 0 ||           \
    ReadBuffer::Cmp((param), "true") == 0 ||            \
    ReadBuffer::Cmp((param), "on") == 0 ||            \
    ReadBuffer::Cmp((param), "1") == 0) ? true : false)

#define SHARD_MIGRATION_WRITER (shardServer->GetShardMigrationWriter())

void ShardHTTPClientSession::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

void ShardHTTPClientSession::SetConnection(HTTPConnection* conn_)
{
    session.SetConnection(conn_);
    conn_->SetOnClose(MFUNC(ShardHTTPClientSession, OnConnectionClose));
}

bool ShardHTTPClientSession::HandleRequest(HTTPRequest& request)
{
    ReadBuffer  cmd;
    
    session.ParseRequest(request, cmd, params);
    return ProcessCommand(cmd);
}

void ShardHTTPClientSession::OnComplete(ClientRequest* request, bool last)
{
    Buffer          tmp;
    ReadBuffer      rb;
    ClientResponse* response;
    Buffer          location;

    response = &request->response;
    switch (response->type)
    {
    case CLIENTRESPONSE_OK:
        session.Print("OK");
        break;
    case CLIENTRESPONSE_NUMBER:
        tmp.Writef("%U", response->number);
        rb.Wrap(tmp);
        session.Print(rb);
        break;
    case CLIENTRESPONSE_VALUE:
        session.Print(response->value);
        break;
    case CLIENTRESPONSE_LIST_KEYS:
        for (unsigned i = 0; i < response->numKeys; i++)
            session.Print(response->keys[i]);
        break;
    case CLIENTRESPONSE_LIST_KEYVALUES:
        for (unsigned i = 0; i < response->numKeys; i++)
            session.PrintPair(response->keys[i], response->values[i]);
        break;
    case CLIENTRESPONSE_NOSERVICE:
        if (GetRedirectedShardServer(request->tableID, request->key, location))
            session.Redirect(location);
        else
            session.Print("NOSERVICE");
        break;
    case CLIENTRESPONSE_FAILED:
        if (GetRedirectedShardServer(request->tableID, request->key, location))
            session.Redirect(location);
        else
            session.Print("FAILED");
        break;
    }
    
    if (last)
    {
        session.Flush();        
        delete request;
    }
}

bool ShardHTTPClientSession::IsActive()
{
    return true;
}

void ShardHTTPClientSession::PrintStatus()
{
    Buffer                      keybuf;
    Buffer                      valbuf;
    uint64_t                    primaryID;
    uint64_t                    totalSpace, freeSpace;
    ShardQuorumProcessor*       it;
    char                        hexbuf[64 + 1];

    session.PrintPair("ScalienDB", "ShardServer");
    session.PrintPair("Version", VERSION_STRING);

    UInt64ToBufferWithBase(hexbuf, sizeof(hexbuf), REPLICATION_CONFIG->GetClusterID(), 64);
    session.PrintPair("ClusterID", hexbuf);

    valbuf.Writef("%U", MY_NODEID);
    valbuf.NullTerminate();
    session.PrintPair("NodeID", valbuf.GetBuffer());   

    session.PrintPair("Controllers", configFile.GetValue("controllers", ""));

    totalSpace = FS_DiskSpace(configFile.GetValue("database.dir", "db"));
    freeSpace = FS_FreeDiskSpace(configFile.GetValue("database.dir", "db"));

    // write quorums, shards, and leases
    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();
    
    valbuf.Writef("%u", quorumProcessors->GetLength());
    valbuf.NullTerminate();
    session.PrintPair("Number of quorums", valbuf.GetBuffer());
    
    FOREACH (it, *quorumProcessors)
    {
        primaryID = shardServer->GetLeaseOwner(it->GetQuorumID());
        
        keybuf.Writef("Quorum %U", it->GetQuorumID());
        keybuf.NullTerminate();

        uint64_t paxosID = shardServer->GetQuorumProcessor(it->GetQuorumID())->GetPaxosID();
        uint64_t highestPaxosID = shardServer->GetQuorumProcessor(it->GetQuorumID())->GetConfigQuorum()->paxosID;
        if (paxosID > highestPaxosID)
            highestPaxosID = paxosID;
        valbuf.Writef("primary %U, paxosID %U/%U", primaryID, paxosID, highestPaxosID);
        if (it->IsCatchupActive())
        {
            valbuf.Appendf(", catchup active (sent: %s/%s, aggregate throughput: %s/s)",
             HUMAN_BYTES(it->GetCatchupBytesSent()),
             HUMAN_BYTES(it->GetCatchupBytesTotal()),
             HUMAN_BYTES(it->GetCatchupThroughput())
             );
        }
        valbuf.NullTerminate();
        
        session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    }    
    
    keybuf.Writef("Migrating shard (sending)");
    keybuf.NullTerminate();
    if (SHARD_MIGRATION_WRITER->IsActive())
        valbuf.Writef("yes (sent: %s/%s, aggregate throughput: %s/s)",
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetBytesSent()),
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetBytesTotal()),
         HUMAN_BYTES(SHARD_MIGRATION_WRITER->GetThroughput()));
    else
        valbuf.Writef("no");
    valbuf.NullTerminate();
    session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    
    session.Flush();
}

void ShardHTTPClientSession::PrintStorage()
{
    Buffer buffer;
    
    shardServer->GetDatabaseManager()->GetEnvironment()->PrintState(
     QUORUM_DATABASE_DATA_CONTEXT, buffer);

    session.Print(buffer);

    session.Flush();
}

bool ShardHTTPClientSession::ProcessCommand(ReadBuffer& cmd)
{
    ClientRequest*  request;
    
    if (HTTP_MATCH_COMMAND(cmd, ""))
    {
        PrintStatus();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "storage"))
    {
        PrintStorage();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "settings"))
    {
        ProcessSettings();
        return true;
    }
    else if (HTTP_MATCH_COMMAND(cmd, "clearcache"))
    {
        StoragePageCache::Clear();
        session.Print("Cache cleared");
        session.Flush();
        return true;
    }

    request = ProcessShardServerCommand(cmd);
    if (!request)
        return false;

    request->session = this;
    shardServer->OnClientRequest(request);
    
    return true;
}

bool ShardHTTPClientSession::ProcessSettings()
{
    ReadBuffer  param;
    bool        boolValue;
    
    if (HTTP_GET_OPT_PARAM(params, "trace", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        Log_SetTrace(boolValue);
        session.PrintPair("Trace", boolValue ? "on" : "off");
    }
    
    if (HTTP_GET_OPT_PARAM(params, "merge", param))
    {
        boolValue = PARAM_BOOL_VALUE(param);
        shardServer->GetDatabaseManager()->GetEnvironment()->SetMergeEnabled(boolValue);
        session.PrintPair("Merge", boolValue ? "on" : "off");
    }
    
    session.Flush();

    return true;
}

ClientRequest* ShardHTTPClientSession::ProcessShardServerCommand(ReadBuffer& cmd)
{
    if (HTTP_MATCH_COMMAND(cmd, "get"))
        return ProcessGet();
    if (HTTP_MATCH_COMMAND(cmd, "set"))
        return ProcessSet();
    if (HTTP_MATCH_COMMAND(cmd, "setifnex"))
        return ProcessSetIfNotExists();
    if (HTTP_MATCH_COMMAND(cmd, "testandset"))
        return ProcessTestAndSet();
    if (HTTP_MATCH_COMMAND(cmd, "getandset"))
        return ProcessGetAndSet();
    if (HTTP_MATCH_COMMAND(cmd, "add"))
        return ProcessAdd();
    if (HTTP_MATCH_COMMAND(cmd, "delete"))
        return ProcessDelete();
    if (HTTP_MATCH_COMMAND(cmd, "remove"))
        return ProcessRemove();
    if (HTTP_MATCH_COMMAND(cmd, "listkeys"))
        return ProcessListKeys();
    if (HTTP_MATCH_COMMAND(cmd, "listkeyvalues"))
        return ProcessListKeyValues();
    if (HTTP_MATCH_COMMAND(cmd, "count"))
        return ProcessCount();
    
    return NULL;
}

ClientRequest* ShardHTTPClientSession::ProcessGet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Get(0, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->Set(0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSetIfNotExists()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->SetIfNotExists(0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessTestAndSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      test;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "test", test);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->TestAndSet(0, tableID, key, test, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessGetAndSet()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->GetAndSet(0, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessAdd()
{
    unsigned        nread;
    uint64_t        tableID;
    int64_t         number;
    ClientRequest*  request;
    ReadBuffer      key;
    ReadBuffer      numberBuffer;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "number", numberBuffer);

    number = BufferToInt64(numberBuffer.GetBuffer(), numberBuffer.GetLength(), &nread);
    if (nread != numberBuffer.GetLength())
        return NULL;

    request = new ClientRequest;
    request->Add(0, tableID, key, number);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessDelete()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Delete(0, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessRemove()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      key;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Remove(0, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessListKeys()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    uint64_t        count;
    uint64_t        offset;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    count = 0;
    HTTP_GET_OPT_U64_PARAM(params, "count", count);
    offset = 0;
    HTTP_GET_OPT_U64_PARAM(params, "offset", offset);

    request = new ClientRequest;
    request->ListKeys(0, tableID, startKey, endKey, count, offset);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessListKeyValues()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    uint64_t        count;
    uint64_t        offset;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    count = 0;
    HTTP_GET_OPT_U64_PARAM(params, "count", count);
    offset = 0;
    HTTP_GET_OPT_U64_PARAM(params, "offset", offset);

    request = new ClientRequest;
    request->ListKeyValues(0, tableID, startKey, endKey, count, offset);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessCount()
{
    ClientRequest*  request;
    uint64_t        tableID;
    ReadBuffer      startKey;
    ReadBuffer      endKey;
    uint64_t        count;
    uint64_t        offset;
    
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_OPT_PARAM(params, "startKey", startKey);
    HTTP_GET_OPT_PARAM(params, "endKey", endKey);
    count = 0;
    HTTP_GET_OPT_U64_PARAM(params, "count", count);
    offset = 0;
    HTTP_GET_OPT_U64_PARAM(params, "offset", offset);

    request = new ClientRequest;
    request->Count(0, tableID, startKey, endKey, count, offset);

    return request;    
}

void ShardHTTPClientSession::OnConnectionClose()
{
    shardServer->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}

bool ShardHTTPClientSession::GetRedirectedShardServer(uint64_t tableID, const ReadBuffer& key, 
 Buffer& location)
{
    ConfigState*        configState;
    ConfigShard*        shard;
    ConfigQuorum*       quorum;
    ConfigShardServer*  server;
    Endpoint            endpoint;
    
    configState = shardServer->GetConfigState();
    shard = configState->GetShard(tableID, key);
    if (!shard)
        return false;
    
    quorum = configState->GetQuorum(shard->quorumID);
    if (!quorum)
        return false;
    
    if (!quorum->hasPrimary)
        return false;
    
    server = configState->GetShardServer(quorum->primaryID);
    if (!server)
        return false;
    
    if (server->nodeID == MY_NODEID)
        return false;
    
    endpoint = server->endpoint;
    endpoint.SetPort(server->httpPort);
    // TODO: change http:// to symbolic const
    location.Writef("http://%s%R", endpoint.ToString(), &session.uri);
    return true;
}
