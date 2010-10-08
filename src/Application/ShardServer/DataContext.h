#ifndef DATACONTEXT_H
#define DATACONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/TotalQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/State/ConfigQuorum.h"
#include "DataMessage.h"

class ShardServer; // forward

/*
===============================================================================================

 DataContext

===============================================================================================
*/

class DataContext : public QuorumContext
{
public:
    void                            Init(ShardServer* shardServer, ConfigQuorum* configQuorum,
                                     StorageTable* quorumTable, uint64_t logCacheSize);
    
    void                            UpdateConfig(ConfigQuorum* configQuorum);
    void                            Append(DataMessage* message);
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

    virtual uint64_t                GetQuorumID();
    virtual void                    SetPaxosID(uint64_t paxosID);
    virtual uint64_t                GetPaxosID();
    virtual uint64_t                GetHighestPaxosID();
    
    virtual Quorum*                 GetQuorum();
    virtual QuorumDatabase*         GetDatabase();
    virtual QuorumTransport*        GetTransport();
    
    virtual void                    OnAppend(ReadBuffer value, bool ownAppend);
    virtual Buffer*                 GetNextValue();
    virtual void                    OnMessage(ReadBuffer msg);
    virtual void                    OnStartCatchup();
    virtual void                    OnCatchupComplete();

    virtual void                    StopReplication();
    virtual void                    ContinueReplication();
    // ========================================================================================

private:
    void                            OnPaxosMessage(ReadBuffer buffer);
    void                            RegisterPaxosID(uint64_t paxosID);

    ShardServer*                    shardServer;
    TotalQuorum                     quorum;
    QuorumDatabase                  database;
    QuorumTransport                 transport;
    ReplicatedLog                   replicatedLog;
    uint64_t                        quorumID;
    uint64_t                        highestPaxosID;
    Buffer                          nextValue;
};

#endif
