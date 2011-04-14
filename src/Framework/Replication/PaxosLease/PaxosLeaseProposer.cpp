#include "PaxosLeaseProposer.h"
#include "PaxosLease.h"
#include "System/Common.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationConfig.h"

PaxosLeaseProposer::~PaxosLeaseProposer()
{
    delete vote;
}

void PaxosLeaseProposer::Init(QuorumContext* context_)
{
    context = context_;
    
    acquireLeaseTimeout.SetCallable(MFUNC(PaxosLeaseProposer, OnAcquireLeaseTimeout));
    acquireLeaseTimeout.SetDelay(PAXOSLEASE_ACQUIRELEASE_TIMEOUT);
    extendLeaseTimeout.SetCallable(MFUNC(PaxosLeaseProposer, OnExtendLeaseTimeout));
    
    vote = NULL;
    highestProposalID = 0;
    state.Init();
}

void PaxosLeaseProposer::OnMessage(PaxosLeaseMessage& imsg)
{
    if (imsg.IsPrepareResponse())
        OnPrepareResponse(imsg);
    else if (imsg.IsProposeResponse())
        OnProposeResponse(imsg);
    else
        ASSERT_FAIL();
}

void PaxosLeaseProposer::Start()
{
    Log_Trace();
    
    EventLoop::Remove(&extendLeaseTimeout);
    
    if (!(state.preparing || state.proposing))
        StartPreparing();
}

void PaxosLeaseProposer::Stop()
{
    Log_Trace();
    
    state.preparing = false;
    state.proposing = false;
    EventLoop::Remove(&extendLeaseTimeout);
    EventLoop::Remove(&acquireLeaseTimeout);
}

uint64_t PaxosLeaseProposer::GetHighestProposalID()
{
    return highestProposalID;
}

void PaxosLeaseProposer::SetHighestProposalID(uint64_t highestProposalID_)
{
    highestProposalID = highestProposalID_;
}

void PaxosLeaseProposer::OnAcquireLeaseTimeout()
{
    Log_Trace();
        
    StartPreparing();
}

void PaxosLeaseProposer::OnExtendLeaseTimeout()
{
    Log_Trace();
    
    ASSERT(!(state.preparing || state.proposing));
    
    StartPreparing();
}

void PaxosLeaseProposer::OnPrepareResponse(PaxosLeaseMessage& imsg)
{
    Log_Trace();

    if (!state.preparing || imsg.proposalID != state.proposalID)
        return;
    
    if (imsg.type == PAXOSLEASE_PREPARE_REJECTED)
        vote->RegisterRejected(imsg.nodeID);
    else
        vote->RegisterAccepted(imsg.nodeID);
    
    if (imsg.type == PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED && 
     imsg.acceptedProposalID >= state.highestReceivedProposalID)
    {
        state.highestReceivedProposalID = imsg.acceptedProposalID;
        state.leaseOwner = imsg.leaseOwner;
    }

    if (vote->IsRejected())
        StartPreparing();
    else if (vote->IsAccepted())
        StartProposing();   
    else if (vote->IsComplete())
        StartPreparing();
}

void PaxosLeaseProposer::OnProposeResponse(PaxosLeaseMessage& imsg)
{
    PaxosLeaseMessage omsg;
    
    Log_Trace();

    if (state.expireTime < Now())
    {
        Log_Trace("already expired, wait for timer");
        return;
    }
    
    if (!state.proposing || imsg.proposalID != state.proposalID)
        return;
    
    if (imsg.type == PAXOSLEASE_PROPOSE_REJECTED)
        vote->RegisterRejected(imsg.nodeID);
    else
        vote->RegisterAccepted(imsg.nodeID);
    
    // see if we have enough positive replies to advance
    if (vote->IsAccepted() && state.expireTime - Now() > 500 /*msec*/)
    {       
        // a majority have accepted our proposal, we have consensus
        EventLoop::Remove(&acquireLeaseTimeout);
        extendLeaseTimeout.SetExpireTime(Now() + (state.expireTime - Now()) / 7);
        Log_Trace("%d", (int)(extendLeaseTimeout.GetExpireTime() - Now()));
        EventLoop::Reset(&extendLeaseTimeout);
    
        omsg.LearnChosen(MY_NODEID, state.leaseOwner,
         (unsigned) (state.expireTime - Now()), state.expireTime, context->GetPaxosID());
        BroadcastMessage(omsg);

        state.proposing = false;
    }
    else if (vote->IsComplete())
        StartPreparing();
}

void PaxosLeaseProposer::BroadcastMessage(PaxosLeaseMessage& omsg)
{
    Log_Trace();
    
    vote->Reset();  
    
    context->GetTransport()->BroadcastMessage(omsg);
}

void PaxosLeaseProposer::StartPreparing()
{
    PaxosLeaseMessage omsg;

    Log_Trace();

    EventLoop::Reset(&acquireLeaseTimeout);

    NewVote();  
    state.proposing = false;
    state.preparing = true;
    state.leaseOwner = MY_NODEID;
    state.highestReceivedProposalID = 0;
    state.proposalID = REPLICATION_CONFIG->NextProposalID(highestProposalID);
        
    if (state.proposalID > highestProposalID)
        highestProposalID = state.proposalID;
    
    omsg.PrepareRequest(MY_NODEID, state.proposalID, context->GetPaxosID());
    BroadcastMessage(omsg);
}

void PaxosLeaseProposer::StartProposing()
{
    PaxosLeaseMessage omsg;
    
    Log_Trace();
    
    state.preparing = false;

    if (state.leaseOwner != MY_NODEID)
        return; // no point in getting someone else a lease, wait for OnAcquireLeaseTimeout

    NewVote();  
    state.proposing = true;
    state.duration = PAXOSLEASE_MAX_LEASE_TIME;
    state.expireTime = Now() + state.duration;
    
    omsg.ProposeRequest(MY_NODEID, state.proposalID, state.leaseOwner, state.duration);
    BroadcastMessage(omsg);
}

void PaxosLeaseProposer::NewVote()
{
    delete vote;
    vote = context->GetQuorum()->NewVote();
}
