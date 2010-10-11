#include "HTTPShardServerSession.h"
#include "ShardServer.h"
#include "System/Config.h"
#include "System/FileSystem.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StorageDataCache.h"
#include "Version.h"

void HTTPShardServerSession::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

void HTTPShardServerSession::SetConnection(HTTPConnection* conn_)
{
    session.SetConnection(conn_);
    conn_->SetOnClose(MFUNC(HTTPShardServerSession, OnConnectionClose));
}

bool HTTPShardServerSession::HandleRequest(HTTPRequest& request)
{
    ReadBuffer  cmd;
    UrlParam    params;
    
    session.ParseRequest(request, cmd, params);
    return ProcessCommand(cmd, params);
}

void HTTPShardServerSession::OnComplete(ClientRequest* request, bool last)
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

bool HTTPShardServerSession::IsActive()
{
    return true;
}

void HTTPShardServerSession::PrintStatus()
{
    ShardServer::QuorumList*    quorums;
    QuorumData*                 quorum;
    Buffer                      keybuf;
    Buffer                      valbuf;
    uint64_t                    primaryID;
    uint64_t                    totalSpace, freeSpace, diskUsage;

    session.PrintPair("ScalienDB", "ShardServer");
    session.PrintPair("Version", VERSION_STRING);

    valbuf.Writef("%U", REPLICATION_CONFIG->GetNodeID());
    valbuf.NullTerminate();
    session.PrintPair("NodeID", valbuf.GetBuffer());   

    session.PrintPair("Controllers", configFile.GetValue("controllers", ""));

    totalSpace = FS_DiskSpace(configFile.GetValue("database.dir", "db"));
    freeSpace = FS_FreeDiskSpace(configFile.GetValue("database.dir", "db"));
    diskUsage = shardServer->GetEnvironment().GetSize();
    valbuf.Writef("%s (Total %s, Free %s)", 
     HumanBytes(diskUsage), HumanBytes(totalSpace), HumanBytes(freeSpace));
    session.PrintPair("Disk usage", valbuf);

    valbuf.Writef("%s (Used %s, %U%%)",
     HumanBytes(DCACHE->GetTotalSize()), HumanBytes(DCACHE->GetUsedSize()),
     (uint64_t) (DCACHE->GetUsedSize() / (float) DCACHE->GetTotalSize() * 100.0 + 0.5));
    session.PrintPair("Cache usage", valbuf);

    // write quorums, shards, and leases
    quorums = shardServer->GetQuorums();
    
    valbuf.Writef("%u", quorums->GetLength());
    valbuf.NullTerminate();
    session.PrintPair("Number of quorums", valbuf.GetBuffer());
    
    for (quorum = quorums->First(); quorum != NULL; quorum = quorums->Next(quorum))
    {
        primaryID = shardServer->GetLeader(quorum->quorumID);
        
        keybuf.Writef("q%U", quorum->quorumID);
        keybuf.NullTerminate();
        
        valbuf.Writef("%U", primaryID);
        valbuf.NullTerminate();
        
        session.PrintPair(keybuf.GetBuffer(), valbuf.GetBuffer());
    }    
    
    session.Flush();
}

bool HTTPShardServerSession::ProcessCommand(ReadBuffer& cmd, UrlParam& params)
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

ClientRequest* HTTPShardServerSession::ProcessShardServerCommand(ReadBuffer& cmd, UrlParam& params)
{
    if (HTTP_MATCH_COMMAND(cmd, "get"))
        return ProcessGet(params);
    if (HTTP_MATCH_COMMAND(cmd, "set"))
        return ProcessSet(params);
    if (HTTP_MATCH_COMMAND(cmd, "delete"))
        return ProcessDelete(params);
    
    return NULL;
}

ClientRequest* HTTPShardServerSession::ProcessGet(UrlParam& params)
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

ClientRequest* HTTPShardServerSession::ProcessSet(UrlParam& params)
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

ClientRequest* HTTPShardServerSession::ProcessDelete(UrlParam& params)
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

void HTTPShardServerSession::OnConnectionClose()
{
    shardServer->OnClientClose(this);
    session.SetConnection(NULL);
    delete this;
}
