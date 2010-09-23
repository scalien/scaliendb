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
    
    Log_Trace("msg.paxosID: %" PRIu64 ", my.paxosID: %" PRIu64 "",
     imsg.paxosID, context->GetPaxosID());

    if (imsg.paxosID < context->GetPaxosID() && imsg.nodeID != context->GetLeader())
        return; // only up-to-date nodes can become masters

//  RLOG->OnPaxosLeaseMessage(msg.paxosID, msg.nodeID); //TODO
    
    if (state.accepted && state.acceptedExpireTime < Now())
    {
        EventLoop::Remove(&leaseTimeout);
        OnLeaseTimeout();
    }
    
    if (imsg.proposalID < state.promisedProposalID)
        omsg.PrepareRejected(REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
    else
    {
        state.promisedProposalID = imsg.proposalID;

        if (!state.accepted)
            omsg.PrepareCurrentlyOpen(REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
        else
            omsg.PreparePreviouslyAccepted(REPLICATION_CONFIG->GetNodeID(), imsg.proposalID,
             state.acceptedProposalID, state.acceptedLeaseOwner, state.acceptedDuration);
    }
    
    context->GetTransport()->SendMessage(imsg.nodeID, omsg, true);
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
        omsg.ProposeRejected(REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
    else
    {
        state.accepted = true;
        state.acceptedProposalID = imsg.proposalID;
        state.acceptedLeaseOwner = imsg.leaseOwner;
        state.acceptedDuration = imsg.duration;
        state.acceptedExpireTime = Now() + state.acceptedDuration;
        
        leaseTimeout.SetExpireTime(state.acceptedExpireTime);
        EventLoop::Reset(&leaseTimeout);
        
        omsg.ProposeAccepted(REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
    }
    
    context->GetTransport()->SendMessage(imsg.nodeID, omsg, true);
}
