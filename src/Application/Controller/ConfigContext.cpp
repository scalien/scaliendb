#include "ConfigContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/Common/CatchupMessage.h"
#include "Controller.h"

void ConfigContext::Init(Controller* controller_, unsigned numControllers, 
 StorageTable* quorumTable, uint64_t logCacheSize)
{
    uint64_t nodeID;
    
    controller = controller_;
    
    quorumID = 0;
    for (nodeID = 0; nodeID < numControllers; nodeID++)
        quorum.AddNode(nodeID);

//  transport.SetPriority(); // TODO
    transport.SetQuorum(&quorum);
    transport.SetQuorumID(quorumID);
    
    database.Init(quorumTable, logCacheSize);
    
    replicatedLog.Init(this);
    paxosLease.Init(this);
    transport.SetQuorumID(quorumID);
    highestPaxosID = 0; 

    paxosLease.AcquireLease();

    replicationActive = true;
}

void ConfigContext::Append(ConfigMessage* message)
{
    message->Write(nextValue);

    replicatedLog.TryAppendNextValue();
}

bool ConfigContext::IsAppending()
{
    return (nextValue.GetLength() != 0);
}

bool ConfigContext::IsLeaseOwner()
{
    return paxosLease.IsLeaseOwner();
}

bool ConfigContext::IsLeaseKnown()
{
    return paxosLease.IsLeaseKnown();
}

uint64_t ConfigContext::GetLeaseOwner()
{
    return paxosLease.GetLeaseOwner();
}

bool ConfigContext::IsLeader()
{
    return IsLeaseOwner() && replicatedLog.IsMultiPaxosEnabled();
}

void ConfigContext::OnLearnLease()
{
    replicatedLog.OnLearnLease();
    controller->OnLearnLease();
}

void ConfigContext::OnLeaseTimeout()
{
    nextValue.Clear();
    replicatedLog.OnLeaseTimeout();
    controller->OnLeaseTimeout();
}

uint64_t ConfigContext::GetQuorumID()
{
    return quorumID;
}

void ConfigContext::SetPaxosID(uint64_t paxosID)
{
    replicatedLog.SetPaxosID(paxosID);
}

uint64_t ConfigContext::GetPaxosID()
{
    return replicatedLog.GetPaxosID();
}

uint64_t ConfigContext::GetHighestPaxosID()
{
    return highestPaxosID;
}

Quorum* ConfigContext::GetQuorum()
{
    return &quorum;
}

QuorumDatabase* ConfigContext::GetDatabase()
{
    return &database;
}

QuorumTransport* ConfigContext::GetTransport()
{
    return &transport;
}

Buffer& ConfigContext::GetNextValue()
{
    return nextValue;
}

void ConfigContext::OnAppend(ReadBuffer value, bool ownAppend)
{
    ConfigMessage message;

    nextValue.Clear();

    assert(message.Read(value));
    controller->OnAppend(message, ownAppend);
}

void ConfigContext::OnMessage(ReadBuffer buffer)
{
    char proto;
    
    Log_Trace("%.*s", P(&buffer));
    
    if (buffer.GetLength() < 2)
        ASSERT_FAIL();

    proto = buffer.GetCharAt(0);
    assert(buffer.GetCharAt(1) == ':');
    
    switch(proto)
    {
        case PAXOSLEASE_PROTOCOL_ID:        // 'L':
            OnPaxosLeaseMessage(buffer);
            break;
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

void ConfigContext::OnStartCatchup()
{
    controller->OnStartCatchup();
}

void ConfigContext::OnCatchupComplete(uint64_t paxosID)
{
    replicatedLog.OnCatchupComplete(paxosID);
}

void ConfigContext::StopReplication()
{
}

void ConfigContext::ContinueReplication()
{
}

void ConfigContext::OnPaxosLeaseMessage(ReadBuffer buffer)
{
    PaxosLeaseMessage msg;

    Log_Trace("%.*s", P(&buffer));
    
    msg.Read(buffer);
    if (msg.type == PAXOSLEASE_PREPARE_REQUEST || msg.type == PAXOSLEASE_LEARN_CHOSEN)
    {
        if (replicationActive)
        {
            RegisterPaxosID(msg.paxosID);
            replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
        }
    }
    paxosLease.OnMessage(msg);
    // TODO: right now PaxosLeaseLearner will not call back
}


void ConfigContext::OnPaxosMessage(ReadBuffer buffer)
{
    PaxosMessage msg;
 
    if (!replicationActive)
        return;
    
    msg.Read(buffer);
    RegisterPaxosID(msg.paxosID);
    replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
    replicatedLog.OnMessage(msg);
}

void ConfigContext::OnCatchupMessage(ReadBuffer buffer)
{
    CatchupMessage msg;
    
    msg.Read(buffer);
    controller->OnCatchupMessage(msg);
}

void ConfigContext::RegisterPaxosID(uint64_t paxosID)
{
    if (paxosID > highestPaxosID)
        highestPaxosID = paxosID;
}
