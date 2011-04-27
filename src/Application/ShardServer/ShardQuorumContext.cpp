#include "ShardQuorumContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void ShardQuorumContext::Init(ConfigQuorum* configQuorum,
 ShardQuorumProcessor* quorumProcessor_)
{
    uint64_t*               it;
    SortedList<uint64_t>    activeNodes;
    
    quorumProcessor = quorumProcessor_;
    quorumID = configQuorum->quorumID;
  
    configQuorum->GetVolatileActiveNodes(activeNodes);

    FOREACH (it, activeNodes)
        quorum.AddNode(*it);

    transport.SetQuorum(&quorum);
    transport.SetQuorumID(quorumID);
    
    database.Init(
     quorumProcessor->GetShardServer()->GetDatabaseManager()->GetQuorumPaxosShard(quorumID),
     quorumProcessor->GetShardServer()->GetDatabaseManager()->GetQuorumLogShard(quorumID));
    
    replicatedLog.Init(this);
    replicatedLog.SetUseProposeTimeouts(false);
    replicatedLog.SetCommitChaining(true);
    replicatedLog.SetAsyncCommit(true);
    transport.SetQuorumID(quorumID);
    highestPaxosID = 0;
    isReplicationActive = true;
}

void ShardQuorumContext::Shutdown()
{
    replicatedLog.Shutdown();
}

//void ShardQuorumContext::SetActiveNodes(SortedList<uint64_t>& activeNodes)
//{
//    unsigned    i;
//    uint64_t*   it;
//    uint64_t*   oldNodes;
//
//    oldNodes = (uint64_t*) quorum.GetNodes();
//
//    if (activeNodes.GetLength() != quorum.GetNumNodes())
//    {
//        ReconfigureQuorum(activeNodes);
//        return;
//    }
//
//    i = 0;
//    FOREACH (it, activeNodes)
//    {
//        if (oldNodes[i] != *it)
//        {
//            ReconfigureQuorum(activeNodes);
//            return;
//        }
//        i++;
//    }
//}

void ShardQuorumContext::SetQuorumNodes(SortedList<uint64_t>& activeNodes)
{
    uint64_t*   it;

    quorum.ClearNodes();
    FOREACH (it, activeNodes)
    {
        Log_Debug("New nodes: %U", *it);
        quorum.AddNode(*it);
    }
}

void ShardQuorumContext::RestartReplication()
{
    replicatedLog.Restart();
}

void ShardQuorumContext::TryReplicationCatchup()
{
    replicatedLog.TryCatchup();
}

void ShardQuorumContext::AppendDummy()
{
    Log_Trace();
    
    replicatedLog.TryAppendDummy();
}

void ShardQuorumContext::Append()
{
    ASSERT(nextValue.GetLength() > 0);
    replicatedLog.TryAppendNextValue();
}

bool ShardQuorumContext::IsAppending()
{
    return (nextValue.GetLength() != 0);
}

void ShardQuorumContext::OnAppendComplete()
{
    replicatedLog.OnAppendComplete();
}

void ShardQuorumContext::NewPaxosRound()
{
    replicatedLog.NewPaxosRound();
}

void ShardQuorumContext::WriteReplicationState()
{
    replicatedLog.WriteState();
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

void ShardQuorumContext::OnIsLeader()
{
    // nothing
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

void ShardQuorumContext::OnStartProposing()
{
    quorumProcessor->OnStartProposing();
}

void ShardQuorumContext::OnAppend(uint64_t paxosID, Buffer& value, bool ownAppend)
{
    nextValue.Clear();

    quorumProcessor->OnAppend(paxosID, value, ownAppend);
}

bool ShardQuorumContext::IsPaxosBlocked()
{
    return quorumProcessor->IsPaxosBlocked();
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
    isReplicationActive = false;
    replicatedLog.Stop();
}

void ShardQuorumContext::ContinueReplication()
{
    isReplicationActive = true;
    replicatedLog.Continue();
}

void ShardQuorumContext::ResetReplicationState()
{
    replicatedLog.ResetPaxosState();
}

void ShardQuorumContext::OnMessage(uint64_t /*nodeID*/, ReadBuffer buffer)
{
    char proto;
    
    Log_Trace("%R", &buffer);
    
    if (buffer.GetLength() < 2)
        ASSERT_FAIL();

    proto = buffer.GetCharAt(0);
    ASSERT(buffer.GetCharAt(1) == ':');
    
    switch(proto)
    {
        case PAXOS_PROTOCOL_ID:             // 'P':
            if (!isReplicationActive)
                break;
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

void ShardQuorumContext::RegisterPaxosID(uint64_t paxosID)
{
    if (paxosID > highestPaxosID)
        highestPaxosID = paxosID;
    
    if (IsLeaseKnown())
        replicatedLog.RegisterPaxosID(paxosID, GetLeaseOwner());
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

//void ShardQuorumContext::ReconfigureQuorum(SortedList<uint64_t>& activeNodes)
//{
//    uint64_t*   it;
//
//    Log_Debug("Reconfiguring quorum");
//    
//    for (unsigned i = 0; i < quorum.GetNumNodes(); i++)
//        Log_Debug("Old nodes: %U", quorum.GetNodes()[i]);
//
//    quorum.ClearNodes();
//    FOREACH (it, activeNodes)
//    {
//        Log_Debug("New nodes: %U", *it);
//        quorum.AddNode(*it);
//    }
//    
//    if (replicatedLog.IsAppending())
//        replicatedLog.Restart();
//}
