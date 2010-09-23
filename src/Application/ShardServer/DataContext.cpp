#include "DataContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "ShardServer.h"

void DataContext::Init(ShardServer* shardServer_, uint64_t quorumID, ConfigQuorum* configQuorum)
{
    uint64_t*   it;
    
    shardServer = shardServer_;
    contextID = quorumID;
  
    for (it = configQuorum->activeNodes.First(); it != NULL; it = configQuorum->activeNodes.Next(it))
        quorum.AddNode(*it);

//  transport.SetPriority(); // TODO
    transport.SetQuorum(&quorum);
    transport.SetContextID(contextID);
    
    
    replicatedLog.Init(this);
    transport.SetContextID(contextID);
    highestPaxosID = 0; 
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
    // TODO: xxx
}

bool DataContext::IsLeader()
{
    // TODO: xxx
}

uint64_t DataContext::GetLeader()
{
    // TODO: xxx
}

void DataContext::OnLearnLease()
{
    ASSERT_FAIL();
}

void DataContext::OnLeaseTimeout()
{
    ASSERT_FAIL();
}

uint64_t DataContext::GetContextID()
{
    return contextID;
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
    ConfigMessage message;

    nextValue.Clear();

    assert(message.Read(value));
    shardServer->OnAppend(message, ownAppend);
}

Buffer* DataContext::GetNextValue()
{
    if (nextValue.GetLength() > 0)
        return &nextValue;
    
    return NULL;
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
