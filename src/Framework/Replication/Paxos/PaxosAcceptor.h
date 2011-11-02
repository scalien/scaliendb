#ifndef PAXOSACCEPTOR_H
#define PAXOSACCEPTOR_H

#include "System/Common.h"
#include "PaxosMessage.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "States/PaxosAcceptorState.h"

class ReplicatedLog; // forward

/*
===============================================================================================

 PaxosAcceptor

===============================================================================================
*/

class PaxosAcceptor
{
public:
    PaxosAcceptor();
        
    void                        Init(QuorumContext* context);
    void                        SetAsyncCommit(bool asyncCommit);
    bool                        GetAsyncCommit();
    
    void                        OnMessage(PaxosMessage& msg);
    void                        OnCatchupComplete();
    void                        WriteState();
    void                        Commit(bool sendReply = false);
    
    void                        ResetState();

    uint64_t                    GetMemoryUsage();

private:
    bool                        OnPrepareRequest(PaxosMessage& msg);
    bool                        OnProposeRequest(PaxosMessage& msg);
    void                        OnStateWritten();

    void                        ReadState();

    bool                        sendReply;
    bool                        asyncCommit;
    QuorumContext*              context;
    PaxosAcceptorState          state;
    PaxosMessage                omsg;
    uint64_t                    senderID;
    uint64_t                    writtenPaxosID;
    Callable                    onStateWritten;
    
    friend class ReplicatedLog;
};

#endif
