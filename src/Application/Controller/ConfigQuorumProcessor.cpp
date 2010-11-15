#include "ConfigQuorumProcessor.h"
#include "Application/Common/ContextTransport.h"
#include "Controller.h"
#include "ConfigHeartbeatManager.h"

void ConfigQuorumProcessor::Init(Controller* controller_,
 unsigned numControllers,  StorageTable* quorumTable)
{
    controller = controller_;
    quorumContext.Init(this, numControllers, quorumTable);
    
    CONTEXT_TRANSPORT->AddQuorumContext(&quorumContext);
}

bool ConfigQuorumProcessor::IsMaster()
{
    return quorumContext.IsLeaseOwner();
}

int64_t ConfigQuorumProcessor::GetMaster()
{
    if (!quorumContext.IsLeaseKnown())
        return -1;

    return (int64_t) quorumContext.GetLeaseOwner();
}

uint64_t ConfigQuorumProcessor::GetQuorumID()
{
    return quorumContext.GetQuorumID();
}

uint64_t ConfigQuorumProcessor::GetPaxosID()
{
    return quorumContext.GetPaxosID();
}

void ConfigQuorumProcessor::TryAppend()
{
    assert(configMessages.GetLength() > 0);
    
    if (!quorumContext.IsAppending())
        quorumContext.Append(configMessages.First());
}

void ConfigQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ConfigMessage*  message;
    uint64_t*       itNodeID;

    if (request->type == CLIENTREQUEST_GET_MASTER)
    {
        if (quorumContext.IsLeaseKnown())
            request->response.Number(quorumContext.GetLeaseOwner());
        else
            request->response.NoService();
            
        request->OnComplete();
        return;
    }
    else if (request->type == CLIENTREQUEST_GET_CONFIG_STATE)
    {
        listenRequests.Append(request);
        request->response.ConfigStateResponse(*controller->GetConfigState());
        request->OnComplete(false);
        return;
    }
    
    if (!quorumContext.IsLeader())
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    if (request->type == CLIENTREQUEST_CREATE_QUORUM)
    {
        // make sure all nodes are currently active
        FOREACH(itNodeID, request->nodes)
        {
            if (!controller->GetHeartbeatManager()->HasHeartbeat(*itNodeID))
            {
                request->response.Failed();
                request->OnComplete();
                return;
            }
        }
    }
    
    message = new ConfigMessage;
    TransformRequest(request, message);
    
    if (!controller->GetConfigState()->CompleteMessage(*message))
    {
        delete message;
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    requests.Append(request);
    configMessages.Append(message);
    TryAppend();
}

void ConfigQuorumProcessor::OnClientClose(ClientSession* session)
{
    ClientRequest*  it;
    ClientRequest*  next;
    
    for (it = listenRequests.First(); it != NULL; it = next)
    {
        next = listenRequests.Next(it);
        if (it->session == session)
        {
            listenRequests.Remove(it);
            it->OnComplete();
        }
    }
}

bool ConfigQuorumProcessor::HasActivateMessage(uint64_t quorumID, uint64_t nodeID)
{
    ConfigMessage *it;
    
    FOREACH(it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_ACTIVATE_SHARDSERVER &&
         it->quorumID == quorumID && it->nodeID == nodeID)
        {
            return true;
        }
    }
    
    return false;
}

bool ConfigQuorumProcessor::HasDeactivateMessage(uint64_t quorumID, uint64_t nodeID)
{
    ConfigMessage *it;
    
    FOREACH(it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_DEACTIVATE_SHARDSERVER &&
         it->quorumID == quorumID && it->nodeID == nodeID)
        {
            return true;
        }
    }
    
    return false;
}

void ConfigQuorumProcessor::ActivateNode(uint64_t quorumID, uint64_t nodeID)
{
    ConfigMessage* message;
 
    if (HasActivateMessage(quorumID, nodeID))
        return;
          
    message = new ConfigMessage();
    message->fromClient = false;
    message->ActivateShardServer(quorumID, nodeID);
    configMessages.Append(message);
    
    TryAppend();
}

void ConfigQuorumProcessor::DeactivateNode(uint64_t quorumID, uint64_t nodeID)
{
    ConfigMessage* message;

    if (HasDeactivateMessage(quorumID, nodeID))
        return;
    
    message = new ConfigMessage();
    message->fromClient = false;
    message->DeactivateShardServer(quorumID, nodeID);
    configMessages.Append(message);
    
    TryAppend();
}

void ConfigQuorumProcessor::OnLearnLease()
{
    // nothing
}

void ConfigQuorumProcessor::OnLeaseTimeout()
{
    ConfigMessage*  itMessage;
    ClientRequest*  itRequest;
 
    // clear config messages   
    for (itMessage = configMessages.First(); itMessage != NULL; 
     itMessage = configMessages.Delete(itMessage))
    {
        /* empty */
    }

    assert(configMessages.GetLength() == 0);

    // clear client requests
    for (itRequest = requests.First(); itRequest != NULL; itRequest = requests.First())
    {
        requests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }
    assert(requests.GetLength() == 0);

    // clear listen requests
    for (itRequest = listenRequests.First(); itRequest != NULL; itRequest = listenRequests.First())
    {
        listenRequests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }
    assert(listenRequests.GetLength() == 0);

    controller->OnLeaseTimeout();
}

void ConfigQuorumProcessor::OnIsLeader()
{
}

void ConfigQuorumProcessor::OnAppend(uint64_t paxosID, ConfigMessage& message, bool ownAppend)
{
}

void ConfigQuorumProcessor::OnStartCatchup()
{
}

void ConfigQuorumProcessor::OnCatchupMessage(CatchupMessage& message)
{
}

void ConfigQuorumProcessor::TransformRequest(ClientRequest* request, ConfigMessage* message)
{
    message->fromClient = true;
    
    switch (request->type)
    {
        case CLIENTREQUEST_CREATE_QUORUM:
            message->type = CONFIGMESSAGE_CREATE_QUORUM;
            message->nodes = request->nodes;
            return;
        case CLIENTREQUEST_CREATE_DATABASE:
            message->type = CONFIGMESSAGE_CREATE_DATABASE;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_RENAME_DATABASE:
            message->type = CONFIGMESSAGE_RENAME_DATABASE;
            message->databaseID = request->databaseID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_DELETE_DATABASE:
            message->type = CONFIGMESSAGE_DELETE_DATABASE;
            message->databaseID = request->databaseID;
            return;
        case CLIENTREQUEST_CREATE_TABLE:
            message->type = CONFIGMESSAGE_CREATE_TABLE;
            message->databaseID = request->databaseID;
            message->quorumID = request->quorumID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_RENAME_TABLE:
            message->type = CONFIGMESSAGE_RENAME_TABLE;
            message->databaseID = request->databaseID;
            message->tableID = request->tableID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_DELETE_TABLE:
            message->type = CONFIGMESSAGE_DELETE_TABLE;
            message->databaseID = request->databaseID;
            message->tableID = request->tableID;
            return;
        default:
            ASSERT_FAIL();
    }
}
