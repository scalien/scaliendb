#include "DataContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "ShardServer.h"

void DataContext::Init(ShardServer* shardServer_, ConfigQuorum* configQuorum,
 StorageTable* table_, uint64_t logCacheSize)
{
    shardServer = shardServer_;
    quorumID = configQuorum->quorumID;
  
    UpdateConfig(configQuorum);
    
//  transport.SetPriority(); // TODO
    transport.SetQuorum(&quorum);
    transport.SetQuorumID(quorumID);
    
    database.Init(table_, logCacheSize);
    
    replicatedLog.Init(this);
    transport.SetQuorumID(quorumID);
    highestPaxosID = 0; 
}

void DataContext::UpdateConfig(ConfigQuorum* configQuorum)
{
    uint64_t*   it;

    quorum.ClearNodes();
    for (it = configQuorum->activeNodes.First(); it != NULL; it = configQuorum->activeNodes.Next(it))
        quorum.AddNode(*it);
}

void DataContext::Append(DataMessage* message)
{
    message->Write(nextValue);

    replicatedLog.TryAppendNextValue();
}

bool DataContext::IsAppending()
{
    return (nextValue.GetLength() != 0);
}

bool DataContext::IsLeaderKnown()
{
    return shardServer->IsLeaderKnown(quorumID);
}

bool DataContext::IsLeader()
{
    return shardServer->IsLeader(quorumID);
}

uint64_t DataContext::GetLeader()
{
    return shardServer->GetLeader(quorumID);
}

void DataContext::OnLearnLease()
{
    replicatedLog.OnLearnLease();
}

void DataContext::OnLeaseTimeout()
{
    nextValue.Clear();
    replicatedLog.OnLeaseTimeout();
}

uint64_t DataContext::GetQuorumID()
{
    return quorumID;
}

void DataContext::SetPaxosID(uint64_t paxosID)
{
    replicatedLog.SetPaxosID(paxosID);
}

uint64_t DataContext::GetPaxosID()
{
    return replicatedLog.GetPaxosID();
}

uint64_t DataContext::GetHighestPaxosID()
{
    return highestPaxosID;
}

Quorum* DataContext::GetQuorum()
{
    return &quorum;
}

QuorumDatabase* DataContext::GetDatabase()
{
    return &database;
}

QuorumTransport* DataContext::GetTransport()
{
    return &transport;
}

void DataContext::OnAppend(ReadBuffer value, bool ownAppend)
{
    DataMessage message;

    nextValue.Clear();

    assert(message.Read(value));
    shardServer->OnAppend(quorumID, message, ownAppend);
}

Buffer* DataContext::GetNextValue()
{
    if (nextValue.GetLength() > 0)
        return &nextValue;
    
    return NULL;
}

void DataContext::OnStartCatchup()
{
    // TODO: xxx
}

void DataContext::OnCatchupComplete()
{
    // TODO: xxx
}

void DataContext::StopReplication()
{
    // TODO: xxx
}

void DataContext::ContinueReplication()
{
    // TODO: xxx
}

void DataContext::OnMessage(ReadBuffer buffer)
{
    char proto;
    
    Log_Trace("%.*s", P(&buffer));
    
    if (buffer.GetLength() < 2)
        ASSERT_FAIL();

    proto = buffer.GetCharAt(0);
    assert(buffer.GetCharAt(1) == ':');
    
    switch(proto)
    {
        case PAXOS_PROTOCOL_ID:             //'P':
            OnPaxosMessage(buffer);
            break;
        default:
            ASSERT_FAIL();
            break;
    }
}

void DataContext::OnPaxosMessage(ReadBuffer buffer)
{
    PaxosMessage msg;
    
    msg.Read(buffer);
    RegisterPaxosID(msg.paxosID);
    replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
    replicatedLog.OnMessage(msg);
}

void DataContext::RegisterPaxosID(uint64_t paxosID)
{
    if (paxosID > highestPaxosID)
        highestPaxosID = paxosID;
}
