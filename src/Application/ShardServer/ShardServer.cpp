#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/ContextTransport.h"

void ShardServer::Init()
{
    unsigned        numControllers;
    uint64_t        nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;

    databaseAdapter.Init(this);
    heartbeatManager.Init(this);
    
    runID = REPLICATION_CONFIG->GetRunID();
    runID += 1;
    REPLICATION_CONFIG->SetRunID(runID);
    REPLICATION_CONFIG->Commit();
    Log_Trace("rundID: %" PRIu64, runID);

    if (MY_NODEID > 0)
        CONTEXT_TRANSPORT->SetSelfNodeID(MY_NODEID);
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
        controllers.Append(nodeID);
        // this will cause the node to connect to the controllers
        // and if my nodeID is not set the MASTER will automatically send
        // me a SetNodeID cluster message
    }

    CONTEXT_TRANSPORT->SetClusterContext(this);    
}

void ShardServer::Shutdown()
{
    databaseAdapter.Shutdown();
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
}

ShardQuorumProcessor* ShardServer::GetQuorumProcessor(uint64_t quorumID)
{
    ShardQuorumProcessor* it;
    
    FOREACH(it, quorumProcessors)
    {
        if (it->GetQuorumID() == quorumID)
            return it;
    }
    
    return NULL;
}

ShardServer::QuorumProcessorList* ShardServer::GetQuorumProcessors()
{
    return &quorumProcessors;
}

ShardDatabaseAdapter* ShardServer::GetDatabaseAdapter()
{
    return &databaseAdapter;
}

ConfigState* ShardServer::GetConfigState()
{
    return &configState;
}

void ShardServer::BroadcastToControllers(Message& message)
{
    uint64_t* itNodeID;

    FOREACH(itNodeID, controllers)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, message);
}

bool ShardServer::IsValidClientRequest(ClientRequest* request)
{
     return request->IsShardServerRequest();
}

void ShardServer::OnClientRequest(ClientRequest* request)
{
    ConfigShard*            shard;
    ShardQuorumProcessor*   quorumProcessor;
    
    shard = configState.GetShard(request->tableID, ReadBuffer(request->key));
    if (!shard)
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }

    quorumProcessor = GetQuorumProcessor(shard->quorumID);
    if (!quorumProcessor)
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    quorumProcessor->OnClientRequest(request);
}

void ShardServer::OnClientClose(ClientSession* /*session*/)
{
    // nothing
}

void ShardServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& message)
{
    ShardQuorumProcessor* quorumProcessor;
    
    Log_Trace();
    
    switch (message.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            if (!CONTEXT_TRANSPORT->IsAwaitingNodeID())
                return;
            CONTEXT_TRANSPORT->SetSelfNodeID(message.nodeID);
            REPLICATION_CONFIG->SetNodeID(message.nodeID);
            REPLICATION_CONFIG->Commit();
            Log_Trace("My nodeID is %" PRIu64 "", message.nodeID);
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            OnSetConfigState(message);
            Log_Trace("Got new configState, master is %d", 
             configState.hasMaster ? (int) configState.masterID : -1);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            quorumProcessor = GetQuorumProcessor(message.quorumID);
            if (quorumProcessor)
                quorumProcessor->OnReceiveLease(message);
            Log_Trace("Recieved lease, quorumID = %" PRIu64 ", proposalID =  %" PRIu64,
             message.quorumID, message.proposalID);
            break;
        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
    // nothing
}

bool ShardServer::OnAwaitingNodeID(Endpoint /*endpoint*/)
{
    // always drop
    return true;
}

bool ShardServer::IsLeaseKnown(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;
    ConfigQuorum*           configQuorum;
    
    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return false;
    
    if (quorumProcessor->IsPrimary())
        return true;
    
    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return false;
    
    if (!configQuorum->hasPrimary)
        return false;
    
    // we already checked this case
    if (configQuorum->primaryID == MY_NODEID)
        return false;
    
    return true;
}

bool ShardServer::IsLeaseOwner(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;

    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return false;

    return quorumProcessor->IsPrimary();
}

uint64_t ShardServer::GetLeaseOwner(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;
    ConfigQuorum*           configQuorum;
    
    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return false;
    
    if (quorumProcessor->IsPrimary())
        return MY_NODEID;

    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return 0;   
    
    if (!configQuorum->hasPrimary)
        return 0;
    
    // we already checked this case
    if (configQuorum->primaryID == MY_NODEID)
        return 0;
    
    return configQuorum->primaryID;
}

void ShardServer::OnSetConfigState(ClusterMessage& message)
{
    bool                    found;
    uint64_t*               itNodeID;
    ConfigQuorum*           configQuorum;
    ShardQuorumProcessor*   quorumProcessor;
    ShardQuorumProcessor*   next;
    
    Log_Trace();

    configState = message.configState;
    
    // look for removal
    for (quorumProcessor = quorumProcessors.First(); quorumProcessor != NULL; quorumProcessor = next)
    {
        configQuorum = configState.GetQuorum(quorumProcessor->GetQuorumID());
        if (configQuorum == NULL)
        {
            // TODO: quorum has disappeared
            ASSERT_FAIL();
        }
        
        found = false;
        next = quorumProcessors.Next(quorumProcessor);
        
        FOREACH(itNodeID, configQuorum->activeNodes)
        {
            if (*itNodeID == MY_NODEID)
            {
                found = true;
                break;
            }
        }
        if (found)
            continue;

        FOREACH(itNodeID, configQuorum->inactiveNodes)
        {
            if (*itNodeID == MY_NODEID)
            {
                found = true;
                break;
            }
        }
        if (found)
            continue;
        
        next = quorumProcessors.Remove(quorumProcessor);
        delete quorumProcessor;
    }

    // check changes in active or inactive node list
    FOREACH(configQuorum, configState.quorums)
    {
        FOREACH(itNodeID, configQuorum->activeNodes)
        {
            if (*itNodeID == MY_NODEID)
            {
                ConfigureQuorum(configQuorum, true); // also creates quorum
                break;
            }
        }

        FOREACH(itNodeID, configQuorum->inactiveNodes)
        {
            if (*itNodeID == MY_NODEID)
            {
                ConfigureQuorum(configQuorum, false); // also creates quorum
                GetQuorumProcessor(configQuorum->quorumID)->TryReplicationCatchup();
                break;
            }
        }
    }
}

void ShardServer::ConfigureQuorum(ConfigQuorum* configQuorum, bool active)
{
    uint64_t                quorumID;
    uint64_t*               itNodeID;
    ConfigShardServer*      shardServer;
    ShardQuorumProcessor*   quorumProcessor;
    
    Log_Trace();    
    
    quorumID = configQuorum->quorumID;

    quorumProcessor = GetQuorumProcessor(quorumID);
    if (active && quorumProcessor == NULL)
    {
        quorumProcessor = new ShardQuorumProcessor;
        quorumProcessor->Init(configQuorum, this);

        quorumProcessors.Append(quorumProcessor);
        FOREACH(itNodeID, configQuorum->activeNodes)
        {
            shardServer = configState.GetShardServer(*itNodeID);
            assert(shardServer != NULL);
            CONTEXT_TRANSPORT->AddNode(*itNodeID, shardServer->endpoint);
        }
    }
    else if (quorumProcessor != NULL)
    {
        quorumProcessor->SetActiveNodes(configQuorum->activeNodes);

        // add nodes to CONTEXT_TRANSPORT
        FOREACH(itNodeID, configQuorum->activeNodes)
        {
            shardServer = configState.GetShardServer(*itNodeID);
            assert(shardServer != NULL);
            CONTEXT_TRANSPORT->AddNode(*itNodeID, shardServer->endpoint);
        }
    }
    
    databaseAdapter.SetShards(configQuorum->shards);
}
