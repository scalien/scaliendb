#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/ContextTransport.h"

void ShardServer::Init()
{
    unsigned        numControllers;
    uint64_t        nodeID;
    const char*     str;
    Endpoint        endpoint;

    if (REPLICATION_CONFIG->GetNodeID() == 0)
        awaitingNodeID = true;
    else
        awaitingNodeID = false;
    
    if (REPLICATION_CONFIG->GetNodeID() > 0)
        CONTEXT_TRANSPORT->SetSelfNodeID(REPLICATION_CONFIG->GetNodeID());
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
        // this will cause the node to connect to the controllers
        // and if my nodeID is not set the MASTER will automatically send
        // me a SetNodeID cluster message
    }

    CONTEXT_TRANSPORT->SetClusterContext(this);
}

void ShardServer::OnAppend(DataMessage& message, bool ownAppend)
{
    // TODO: xxx
}

bool ShardServer::IsValidClientRequest(ClientRequest* request)
{
     return request->IsShardServerRequest();
}

void ShardServer::OnClientRequest(ClientRequest* request)
{
    DataMessage*    message;
    ConfigShard*    shard;
    QuorumData*     quorumData;
    
    shard = configState.GetShard(request->tableID, ReadBuffer(request->key));
    if (!shard)
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }

    quorumData = LocateQuorum(shard->quorumID);
    if (!quorumData)
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    if (request->type == CLIENTREQUEST_GET)
    {
        // TODO: xxx
        return;
    }

    message = new DataMessage;
    FromClientRequest(request, message);
    
    quorumData->requests.Append(request);
    quorumData->dataMessages.Append(message);
    TryAppend(quorumData);
}

void ShardServer::OnClientClose(ClientSession* /*session*/)
{
}

void ShardServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& msg)
{
    Log_Trace();
    
    switch (msg.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            if (!awaitingNodeID)
                return;
            awaitingNodeID = false;
            CONTEXT_TRANSPORT->SetSelfNodeID(msg.nodeID);
            REPLICATION_CONFIG->SetNodeID(msg.nodeID);
            REPLICATION_CONFIG->Commit();
            Log_Trace("my nodeID is %" PRIu64 "", msg.nodeID);
            break;
        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
    // nothing
}

void ShardServer::OnAwaitingNodeID(Endpoint /*endpoint*/)
{
    // nothing
}

QuorumData* ShardServer::LocateQuorum(uint64_t quorumID)
{
    // TODO:
}

void ShardServer::TryAppend(QuorumData* quorumData)
{
}

void ShardServer::FromClientRequest(ClientRequest* request, DataMessage* message)
{
    switch (request->type)
    {
        case CLIENTREQUEST_SET:
            message->type = DATAMESSAGE_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            return;
        case CLIENTREQUEST_DELETE:
            message->type = DATAMESSAGE_DELETE;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            return;
        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnRequestLeaseTimeout()
{
    QuorumData* quorum;
    
    for (quorum = quorums.First(); quorum != NULL; quorum = quorums.Next(quorum))
    {
        if (!quorum->isPrimary)
        {
            // TODO: try to get lease
            
        }
    }
}
