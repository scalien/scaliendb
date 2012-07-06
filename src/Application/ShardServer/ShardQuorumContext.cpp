#include "ShardQuorumContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

#define PAXOS_QUEUE_LENGTH  1000

void ShardQuorumContext::Init(ConfigQuorum* configQuorum,
 ShardQuorumProcessor* quorumProcessor_)
{
    uint64_t*               it;
    SortedList<uint64_t>    activeNodes;
    
    quorumProcessor = quorumProcessor_;
    quorumID = configQuorum->quorumID;
    numPendingPaxos = 0;
  
    configQuorum->GetVolatileActiveNodes(activeNodes);

    FOREACH (it, activeNodes)
        quorum.AddNode(*it);

    transport.SetQuorum(&quorum);
    transport.SetQuorumID(quorumID);
    
    database.Init(this,
     quorumProcessor->GetShardServer()->GetDatabaseManager()->GetQuorumPaxosShard(quorumID),
     quorumProcessor->GetShardServer()->GetDatabaseManager()->GetQuorumLogShard(quorumID));
    
    replicatedLog.Init(this);
    transport.SetQuorumID(quorumID);
    highestPaxosID = 0;
    isReplicationActive = true;
}

void ShardQuorumContext::Shutdown()
{
    replicatedLog.Shutdown();
}

void ShardQuorumContext::SetQuorumNodes(SortedList<uint64_t>& activeNodes)
{
    uint64_t*   it;

    quorum.ClearNodes();
    FOREACH (it, activeNodes)
        quorum.AddNode(*it);
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

void ShardQuorumContext::WriteReplicationState()
{
    replicatedLog.WriteState();
}

uint64_t ShardQuorumContext::GetMemoryUsage()
{
    return nextValue.GetSize() + replicatedLog.GetMemoryUsage();
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

uint64_t ShardQuorumContext::GetLastLearnChosenTime()
{
    return replicatedLog.GetLastLearnChosenTime();
}

uint64_t ShardQuorumContext::GetReplicationThroughput()
{
    return replicatedLog.GetReplicationThroughput();
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

bool ShardQuorumContext::UseSyncCommit()
{
    return false; // async
}

bool ShardQuorumContext::UseProposeTimeouts()
{
    return false;
}

bool ShardQuorumContext::UseCommitChaining()
{
    return true;
}

bool ShardQuorumContext::AlwaysUseDatabaseCatchup()
{
    return false;
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

void ShardQuorumContext::OnCatchupStarted()
{
    replicatedLog.OnCatchupStarted();
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

bool ShardQuorumContext::IsWaitingOnAppend()
{
    return replicatedLog.IsWaitingOnAppend();
}

void ShardQuorumContext::OnMessage(ReadBuffer buffer)
{
    char    proto;
    Buffer* message;
    
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
            if (numPendingPaxos == 0)
            {
                numPendingPaxos++;
                OnPaxosMessage(buffer);
            }
            else
            {
                if (paxosMessageQueue.GetLength() < PAXOS_QUEUE_LENGTH)
                {
                    message = new Buffer;
                    message->Write(buffer);
                    paxosMessageQueue.Enqueue(message);
                }
                else
                {
                    Log_Debug("Dropping paxos msg because queue is too long. Queue stuck?");
                }
            }
            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

void ShardQuorumContext::OnMessageProcessed()
{
    ReadBuffer  buffer;
    Buffer*     message;

    ASSERT(numPendingPaxos == 1);
    numPendingPaxos--;

    if (!isReplicationActive)
        paxosMessageQueue.DeleteQueue(); // drop messages
    
    if (paxosMessageQueue.GetLength() > 0)
    {
        message = paxosMessageQueue.Dequeue();
        buffer.Wrap(*message);
        OnMessage(buffer);
    }
}

void ShardQuorumContext::RegisterPaxosID(uint64_t paxosID)
{
    if (paxosID > highestPaxosID)
        highestPaxosID = paxosID;
 
    // ReplicatedLog::RegisterPaxosID() will perform a RequestChosen()
    // so don't call it if replication is inactive,
    // because catchup is already running
    if (isReplicationActive && IsLeaseKnown())
        replicatedLog.RegisterPaxosID(paxosID, GetLeaseOwner());
}

void ShardQuorumContext::OnPaxosMessage(ReadBuffer buffer)
{
    PaxosMessage msg;
    
    msg.Read(buffer);
    
    if (msg.IsPaxosRequest() || msg.IsPaxosResponse() || msg.IsLearn())
    {
        if (!quorum.IsMember(msg.nodeID))
        {
            Log_Debug("Dropping paxos msg from %U because that node is not a quourm member", msg.nodeID);
            OnMessageProcessed();
            return;
        }
    }

    RegisterPaxosID(msg.paxosID);
    replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
    replicatedLog.OnMessage(msg);
}
