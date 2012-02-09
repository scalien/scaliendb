#ifndef PAXOSLEASEACCEPTOR_H
#define PAXOSLEASEACCEPTOR_H

#include "System/Common.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
#include "States/PaxosLeaseAcceptorState.h"
#include "PaxosLeaseMessage.h"

/*
===============================================================================================

 PaxosLeaseAcceptor

===============================================================================================
*/

class PaxosLeaseAcceptor
{
    typedef PaxosLeaseAcceptorState AcceptorState;
public:
    void            Init(QuorumContext* context);
    void            OnMessage(PaxosLeaseMessage& msg);

private:
    void            OnLeaseTimeout();
    void            OnPrepareRequest(PaxosLeaseMessage& msg);
    void            OnProposeRequest(PaxosLeaseMessage& msg);

    QuorumContext*  context;
    AcceptorState   state;
    Timer           leaseTimeout;
};

#endif
