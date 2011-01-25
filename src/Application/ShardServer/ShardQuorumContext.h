#ifndef SHARDQUORUMCONTEXT_H
#define SHARDQUORUMCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/TotalQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/ConfigState/ConfigQuorum.h"
#include "ShardMessage.h"

class ShardQuorumProcessor; // forward

/*
===============================================================================================

 ShardQuorumContext

===============================================================================================
*/

class ShardQuorumContext : public QuorumContext
{
public:
    void                            Init(ConfigQuorum* configQuorum,
                                     ShardQuorumProcessor* quorumProcessor_);
    
    void                            SetActiveNodes(List<uint64_t>& activeNodes);
    void                            TryReplicationCatchup();
    void                            AppendDummy();
    void                            Append(); // nextValue was filled up using GetNextValue()
    bool                            IsAppending();
    void                            OnAppendComplete();
    
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
    
    virtual void                    OnAppend(uint64_t paxosID, ReadBuffer value, bool ownAppend);
    virtual bool                    IsPaxosBlocked();
    virtual Buffer&                 GetNextValue();
    virtual void                    OnMessage(uint64_t nodeID, ReadBuffer msg);
    virtual void                    OnStartCatchup();
    virtual void                    OnCatchupComplete(uint64_t paxosID);

    virtual void                    StopReplication();
    virtual void                    ContinueReplication();

    void                            RegisterPaxosID(uint64_t paxosID);

private:
    void                            OnPaxosMessage(ReadBuffer buffer);
    void                            OnCatchupMessage(ReadBuffer buffer);

    bool                            isReplicationActive;
    uint64_t                        quorumID;
    uint64_t                        highestPaxosID;
    ShardQuorumProcessor*           quorumProcessor;
    TotalQuorum                     quorum;
    QuorumDatabase                  database;
    QuorumTransport                 transport;
    ReplicatedLog                   replicatedLog;
    Buffer                          nextValue;
};

#endif
