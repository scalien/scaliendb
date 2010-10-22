#include "ShardQuorumContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void ShardQuorumContext::Init(ConfigQuorum* configQuorum,
 ShardQuorumProcessor* quorumProcessor_, StorageTable* table)
{
    ConfigQuorum::NodeList activeNodes;
    
    quorumProcessor = quorumProcessor_;
    quorumID = configQuorum->quorumID;
  
    activeNodes = configQuorum->GetVolatileActiveNodes();
    SetActiveNodes(activeNodes);

    transport.SetQuorum(&quorum);
    transport.SetQuorumID(quorumID);
    
    database.Init(table);
    
    replicatedLog.Init(this);
    transport.SetQuorumID(quorumID);
    highestPaxosID = 0;
}

void ShardQuorumContext::SetActiveNodes(ConfigQuorum::NodeList& activeNodes)
{
    uint64_t*   it;

    Log_Trace("Reconfiguring quorum");

    quorum.ClearNodes();
    FOREACH(it, activeNodes)
    {
        Log_Trace("Adding %" PRIu64 "", *it);
        quorum.AddNode(*it);
    }
}

void ShardQuorumContext::Append()
{
    assert(nextValue.GetLength() > 0);
    replicatedLog.TryAppendNextValue();
}

bool ShardQuorumContext::IsAppending()
{
    return (nextValue.GetLength() != 0);
}

bool ShardQuorumContext::IsLeaseOwner()
{
    return quorumProcessor->IsPrimary();
}

bool ShardQuorumContext::IsLeaseKnown()
{
    return quorumProcessor->GetShardServer()->IsLeaseKnown(quorumID);
}

uint64_t ShardQuorumContext::GetLeaseOwner()
{
    return quorumProcessor->GetShardServer()->GetLeaseOwner(quorumID);
}

bool ShardQuorumContext::IsLeader()
{
    return IsLeaseOwner() && replicatedLog.IsMultiPaxosEnabled();
}

void ShardQuorumContext::OnLearnLease()
{
    replicatedLog.OnLearnLease();
}

void ShardQuorumContext::OnLeaseTimeout()
{
    nextValue.Clear();
    replicatedLog.OnLeaseTimeout();
}

uint64_t ShardQuorumContext::GetQuorumID()
{
    return quorumID;
}

void ShardQuorumContext::SetPaxosID(uint64_t paxosID)
{
    replicatedLog.SetPaxosID(paxosID);
}

uint64_t ShardQuorumContext::GetPaxosID()
{
    return replicatedLog.GetPaxosID();
}

uint64_t ShardQuorumContext::GetHighestPaxosID()
{
    return highestPaxosID;
}

Quorum* ShardQuorumContext::GetQuorum()
{
    return &quorum;
}

QuorumDatabase* ShardQuorumContext::GetDatabase()
{
    return &database;
}

QuorumTransport* ShardQuorumContext::GetTransport()
{
    return &transport;
}

void ShardQuorumContext::OnAppend(ReadBuffer value, bool ownAppend)
{
    nextValue.Clear();

    quorumProcessor->OnAppend(value, ownAppend);
}

Buffer& ShardQuorumContext::GetNextValue()
{
    return nextValue;
}

void ShardQuorumContext::OnStartCatchup()
{
    quorumProcessor->OnStartCatchup();
}

void ShardQuorumContext::OnCatchupComplete(uint64_t paxosID)
{
    replicatedLog.OnCatchupComplete(paxosID);
}

void ShardQuorumContext::StopReplication()
{
    // TODO: xxx
}

void ShardQuorumContext::ContinueReplication()
{
    // TODO: xxx
}

void ShardQuorumContext::OnMessage(uint64_t /*nodeID*/, ReadBuffer buffer)
{
    char proto;
    
    Log_Trace("%.*s", P(&buffer));
    
    if (buffer.GetLength() < 2)
        ASSERT_FAIL();

    proto = buffer.GetCharAt(0);
    assert(buffer.GetCharAt(1) == ':');
    
    switch(proto)
    {
        case PAXOS_PROTOCOL_ID:             // 'P':
            OnPaxosMessage(buffer);
            break;
        case CATCHUP_PROTOCOL_ID:           // 'C'
            OnCatchupMessage(buffer);
            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

void ShardQuorumContext::OnPaxosMessage(ReadBuffer buffer)
{
    PaxosMessage msg;
    
    msg.Read(buffer);
    RegisterPaxosID(msg.paxosID);
    replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
    replicatedLog.OnMessage(msg);
}

void ShardQuorumContext::OnCatchupMessage(ReadBuffer buffer)
{
    CatchupMessage msg;
    
    msg.Read(buffer);
    quorumProcessor->OnCatchupMessage(msg);
}

void ShardQuorumContext::RegisterPaxosID(uint64_t paxosID)
{
    if (paxosID > highestPaxosID)
        highestPaxosID = paxosID;
}
