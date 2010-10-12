#ifndef CONFIGCONTEXT_H
#define CONFIGCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/MajorityQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "ConfigMessage.h"

class Controller; // forward

/*
===============================================================================================

 ConfigContext

===============================================================================================
*/

class ConfigContext : public QuorumContext
{
public:
    void                            Init(Controller* controller, unsigned numControllers, 
                                     StorageTable* quorumTable, uint64_t logCacheSize);
    
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

    virtual uint64_t                GetQuorumID();
    virtual void                    SetPaxosID(uint64_t paxosID);
    virtual uint64_t                GetPaxosID();
    virtual uint64_t                GetHighestPaxosID();
    
    virtual Quorum*                 GetQuorum();
    virtual QuorumDatabase*         GetDatabase();
    virtual QuorumTransport*        GetTransport();
    
    virtual Buffer*                 GetNextValue();

    virtual void                    OnAppend(ReadBuffer value, bool ownAppend);
    virtual void                    OnMessage(ReadBuffer msg);
    virtual void                    OnStartCatchup();
    virtual void                    OnCatchupComplete(uint64_t paxosID);

    virtual void                    StopReplication();
    virtual void                    ContinueReplication();
    // ========================================================================================

private:
    void                            OnPaxosLeaseMessage(ReadBuffer buffer);
    void                            OnPaxosMessage(ReadBuffer buffer);
    void                            OnCatchupMessage(ReadBuffer buffer);
    void                            RegisterPaxosID(uint64_t paxosID);

    Controller*                     controller;
    MajorityQuorum                  quorum;
    QuorumDatabase                  database;
    QuorumTransport                 transport;
    ReplicatedLog                   replicatedLog;
    PaxosLease                      paxosLease;
    uint64_t                        quorumID;
    uint64_t                        highestPaxosID;
    Buffer                          nextValue;
    bool                            replicationActive;
};

#endif
