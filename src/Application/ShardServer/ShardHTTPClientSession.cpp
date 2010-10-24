#include "ShardHTTPClientSession.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "System/FileSystem.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StorageDataCache.h"
#include "Version.h"

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
    UrlParam    params;
    
    session.ParseRequest(request, cmd, params);
    return ProcessCommand(cmd, params);
}

void ShardHTTPClientSession::OnComplete(ClientRequest* request, bool last)
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

bool ShardHTTPClientSession::IsActive()
{
    return true;
}

void ShardHTTPClientSession::PrintStatus()
{
    Buffer                      keybuf;
    Buffer                      valbuf;
    uint64_t                    primaryID;
    uint64_t                    totalSpace, freeSpace, diskUsage;
    ShardQuorumProcessor*       it;

    session.PrintPair("ScalienDB", "ShardServer");
    session.PrintPair("Version", VERSION_STRING);

    valbuf.Writef("%U", MY_NODEID);
    valbuf.NullTerminate();
    session.PrintPair("NodeID", valbuf.GetBuffer());   

    session.PrintPair("Controllers", configFile.GetValue("controllers", ""));

    totalSpace = FS_DiskSpace(configFile.GetValue("database.dir", "db"));
    freeSpace = FS_FreeDiskSpace(configFile.GetValue("database.dir", "db"));
    diskUsage = shardServer->GetDatabaseAdapter()->GetEnvironment()->GetSize();
    valbuf.Writef("%s (Total %s, Free %s)", 
     HumanBytes(diskUsage), HumanBytes(totalSpace), HumanBytes(freeSpace));
    session.PrintPair("Disk usage", valbuf);

    valbuf.Writef("%s (Used %s, %U%%)",
     HumanBytes(DCACHE->GetTotalSize()), HumanBytes(DCACHE->GetUsedSize()),
     (uint64_t) (DCACHE->GetUsedSize() / (float) DCACHE->GetTotalSize() * 100.0 + 0.5));
    session.PrintPair("Cache usage", valbuf);

    // write quorums, shards, and leases
    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();

    
    valbuf.Writef("%u", quorumProcessors->GetLength());
    valbuf.NullTerminate();
    session.PrintPair("Number of quorums", valbuf.GetBuffer());
    
    FOREACH(it, *quorumProcessors)
    {
        primaryID = shardServer->GetLeaseOwner(it->GetQuorumID());
        
        keybuf.Writef("q%U", it->GetQuorumID());
        keybuf.NullTerminate();
        
        valbuf.Writef("%U", primaryID);
        valbuf.NullTerminate();
        
        session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    }    
    
    session.Flush();
}

bool ShardHTTPClientSession::ProcessCommand(ReadBuffer& cmd, UrlParam& params)
{
    ClientRequest*  request;
    
    if (HTTP_MATCH_COMMAND(cmd, ""))
    {
        PrintStatus();
        return true;
    }

    request = ProcessShardServerCommand(cmd, params);
    if (!request)
        return false;

    request->session = this;
    shardServer->OnClientRequest(request);
    
    return true;
}

ClientRequest* ShardHTTPClientSession::ProcessShardServerCommand(ReadBuffer& cmd, UrlParam& params)
{
    if (HTTP_MATCH_COMMAND(cmd, "get"))
        return ProcessGet(params);
    if (HTTP_MATCH_COMMAND(cmd, "set"))
        return ProcessSet(params);
    if (HTTP_MATCH_COMMAND(cmd, "setifnex"))
        return ProcessSetIfNotExists(params);
    if (HTTP_MATCH_COMMAND(cmd, "testandset"))
        return ProcessTestAndSet(params);
    if (HTTP_MATCH_COMMAND(cmd, "delete"))
        return ProcessDelete(params);
    if (HTTP_MATCH_COMMAND(cmd, "remove"))
        return ProcessRemove(params);
    
    return NULL;
}

ClientRequest* ShardHTTPClientSession::ProcessGet(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Get(0, databaseID, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSet(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->Set(0, databaseID, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessSetIfNotExists(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->SetIfNotExists(0, databaseID, tableID, key, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessTestAndSet(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      test;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);
    HTTP_GET_PARAM(params, "test", test);
    HTTP_GET_PARAM(params, "value", value);

    request = new ClientRequest;
    request->TestAndSet(0, databaseID, tableID, key, test, value);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessDelete(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Delete(0, databaseID, tableID, key);

    return request;    
}

ClientRequest* ShardHTTPClientSession::ProcessRemove(UrlParam& params)
{
    ClientRequest*  request;
    uint64_t        databaseID;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
    HTTP_GET_U64_PARAM(params, "tableID", tableID);
    HTTP_GET_PARAM(params, "key", key);

    request = new ClientRequest;
    request->Remove(0, databaseID, tableID, key);

    return request;    
}

void ShardHTTPClientSession::OnConnectionClose()
{
    shardServer->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}
