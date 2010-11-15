#include "ConfigQuorumProcessor.h"
#include "Controller.h"
#include "ConfigHeartbeatManager.h"

void ConfigQuorumProcessor::Init(Controller* controller_,
 unsigned numControllers,  StorageTable* quorumTable)
{
    controller = controller_;
    quorumContext.Init(this, numControllers, quorumTable);
}


bool ConfigQuorumProcessor::IsMaster()
{
    return quorumContext.IsLeaseOwner();
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
