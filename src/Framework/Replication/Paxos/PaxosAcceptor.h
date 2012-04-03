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
    typedef PaxosAcceptorState State;

public:
    PaxosAcceptor();
        
    void            Init(QuorumContext* context);
    bool            OnPrepareRequest(PaxosMessage& msg);
    bool            OnProposeRequest(PaxosMessage& msg);
    void            OnCatchupStarted();
    void            OnCatchupComplete();
    void            WriteState();
    uint64_t        GetMemoryUsage();

    State           state;

private:
    void            Commit();
    void            OnStateWritten();
    void            ReadState();
    bool            TestRejection(PaxosMessage& msg);
    void            AcceptPrepareRequest(PaxosMessage& msg);
    void            AcceptProposeRequest(PaxosMessage& msg);

    bool            isCommitting;
    QuorumContext*  context;
    PaxosMessage    omsg;
    uint64_t        senderID;
    uint64_t        writtenPaxosID;
    Callable        onStateWritten;    
};

#endif
