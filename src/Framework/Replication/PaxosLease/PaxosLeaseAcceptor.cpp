#include "PaxosLeaseAcceptor.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationConfig.h"

void PaxosLeaseAcceptor::Init(QuorumContext* context_)
{
    context = context_;

    leaseTimeout.SetCallable(MFUNC(PaxosLeaseAcceptor, OnLeaseTimeout));

    state.Init();
}

void PaxosLeaseAcceptor::OnMessage(PaxosLeaseMessage& imsg)
{
    if (imsg.type == PAXOSLEASE_PREPARE_REQUEST)
        OnPrepareRequest(imsg);
    else if (imsg.type == PAXOSLEASE_PROPOSE_REQUEST)
        OnProposeRequest(imsg);
    else
        ASSERT_FAIL();
}

void PaxosLeaseAcceptor::OnLeaseTimeout()
{
    Log_Trace();
    
    state.OnLeaseTimeout();
}

void PaxosLeaseAcceptor::OnPrepareRequest(PaxosLeaseMessage& imsg)
{
    PaxosLeaseMessage omsg;
    
    Log_Trace("msg.paxosID: %U, my.paxosID: %U", imsg.paxosID, context->GetPaxosID());

    if (imsg.paxosID < context->GetPaxosID() && imsg.nodeID != context->GetLeaseOwner())
        return; // only up-to-date nodes can become masters

//  RLOG->OnPaxosLeaseMessage(msg.paxosID, msg.nodeID); //TODO
    
    if (state.accepted && state.acceptedExpireTime < Now())
    {
        EventLoop::Remove(&leaseTimeout);
        OnLeaseTimeout();
    }
    
    if (imsg.proposalID < state.promisedProposalID)
        omsg.PrepareRejected(MY_NODEID, imsg.proposalID);
    else
    {
        state.promisedProposalID = imsg.proposalID;

        if (!state.accepted)
            omsg.PrepareCurrentlyOpen(MY_NODEID, imsg.proposalID);
        else
            omsg.PreparePreviouslyAccepted(MY_NODEID, imsg.proposalID,
             state.acceptedProposalID, state.acceptedLeaseOwner, state.acceptedDuration);
    }
    
    context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}

void PaxosLeaseAcceptor::OnProposeRequest(PaxosLeaseMessage& imsg)
{
    PaxosLeaseMessage omsg;

    Log_Trace();
    
    if (state.accepted && state.acceptedExpireTime < Now())
    {
        EventLoop::Remove(&leaseTimeout);
        OnLeaseTimeout();
    }
    
    if (imsg.proposalID < state.promisedProposalID)
        omsg.ProposeRejected(MY_NODEID, imsg.proposalID);
    else
    {
        state.accepted = true;
        state.acceptedProposalID = imsg.proposalID;
        state.acceptedLeaseOwner = imsg.leaseOwner;
        state.acceptedDuration = imsg.duration;
        state.acceptedExpireTime = Now() + state.acceptedDuration;
        
        leaseTimeout.SetExpireTime(state.acceptedExpireTime);
        EventLoop::Reset(&leaseTimeout);
        
        omsg.ProposeAccepted(MY_NODEID, imsg.proposalID);
    }
    
    context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}
