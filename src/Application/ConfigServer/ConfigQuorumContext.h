#ifndef CONFIGQUORUMCONTEXT_H
#define CONFIGQUORUMCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/MajorityQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "ConfigMessage.h"

class ConfigQuorumProcessor; // forward

/*
===============================================================================================

 ConfigQuorumContext

===============================================================================================
*/

class ConfigQuorumContext : public QuorumContext
{
public:
    void                            Init(ConfigQuorumProcessor* quorumProcessor,
                                     unsigned numConfigServers,
                                     StorageShardProxy* quorumPaxosShard,
                                     StorageShardProxy* quorumLogShard);
    
    void                            Append(ConfigMessage* message);
    bool                            IsAppending();
    
    // ========================================================================================
    // QuorumContext interface:
    //
    virtual bool                    IsLeaseOwner();
    virtual bool                    IsLeaseKnown();
    virtual uint64_t                GetLeaseOwner();
    // multiPaxos
    virtual bool                    IsLeader();

    virtual void                    OnLearnLease();
    virtual void                    OnLeaseTimeout();
    virtual void                    OnIsLeader();

    virtual uint64_t                GetQuorumID();
    virtual void                    SetPaxosID(uint64_t paxosID);
    virtual uint64_t                GetPaxosID();
    virtual uint64_t                GetHighestPaxosID();
    
    virtual Quorum*                 GetQuorum();
    virtual QuorumDatabase*         GetDatabase();
    virtual QuorumTransport*        GetTransport();
    
    virtual bool                    IsPaxosBlocked();
    virtual Buffer&                 GetNextValue();

    virtual void                    OnStartProposing();
    virtual void                    OnAppend(uint64_t paxosID, Buffer& value, bool ownAppend);
    virtual void                    OnMessage(ReadBuffer msg);
    virtual void                    OnMessageProcessed() {}
    virtual void                    OnStartCatchup();
    virtual void                    OnCatchupComplete(uint64_t paxosID);

    void                            OnAppendComplete();

    virtual void                    StopReplication();
    virtual void                    ContinueReplication();

private:
    void                            OnPaxosLeaseMessage(ReadBuffer buffer);
    void                            OnPaxosMessage(ReadBuffer buffer);
    void                            OnCatchupMessage(ReadBuffer buffer);
    void                            RegisterPaxosID(uint64_t paxosID);

    bool                            isReplicationActive;
    uint64_t                        quorumID;
    uint64_t                        highestPaxosID;
    ConfigQuorumProcessor*          quorumProcessor;
    MajorityQuorum                  quorum;
    QuorumDatabase                  database;
    QuorumTransport                 transport;
    ReplicatedLog                   replicatedLog;
    PaxosLease                      paxosLease;
    Buffer                          nextValue;
};

#endif
