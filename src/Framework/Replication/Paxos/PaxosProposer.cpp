#include "PaxosProposer.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationConfig.h"

PaxosProposer::~PaxosProposer()
{
    delete vote;
}

void PaxosProposer::Init(QuorumContext* context_)
{
    context = context_;
    
    prepareTimeout.SetCallable(MFUNC(PaxosProposer, OnPrepareTimeout));
    prepareTimeout.SetDelay(PAXOS_ROUND_TIMEOUT);
    proposeTimeout.SetCallable(MFUNC(PaxosProposer, OnProposeTimeout));
    proposeTimeout.SetDelay(PAXOS_ROUND_TIMEOUT);
    restartTimeout.SetCallable(MFUNC(PaxosProposer, OnRestartTimeout));
    restartTimeout.SetDelay(PAXOS_RESTART_TIMEOUT);

    vote = NULL;
    state.Init();
}

void PaxosProposer::Shutdown()
{
    if (prepareTimeout.IsActive())
        EventLoop::Remove(&prepareTimeout);

    if (proposeTimeout.IsActive())
        EventLoop::Remove(&proposeTimeout);

    if (restartTimeout.IsActive())
        EventLoop::Remove(&restartTimeout);
}

void PaxosProposer::SetUseTimeouts(bool useTimeouts_)
{
    useTimeouts = useTimeouts_;
}

void PaxosProposer::OnMessage(PaxosMessage& imsg)
{
    if (imsg.IsPrepareResponse())
        OnPrepareResponse(imsg);
    else if (imsg.IsProposeResponse())
        OnProposeResponse(imsg);
    else
        ASSERT_FAIL();
}

void PaxosProposer::OnPrepareTimeout()
{
    Log_Debug("OnPrepareTimeout");
    Log_Trace();
    
    ASSERT(state.preparing);

    if (context->IsPaxosBlocked())
    {
        EventLoop::Add(&prepareTimeout);
        return;
    }
    
    if (useTimeouts || vote->IsRejected())
        StartPreparing();
    else
        EventLoop::Add(&prepareTimeout);
}

void PaxosProposer::OnProposeTimeout()
{
    Log_Debug("OnProposeTimeout");
    Log_Trace();
    
    ASSERT(state.proposing);

    if (context->IsPaxosBlocked())
    {
        EventLoop::Add(&proposeTimeout);
        return;
    }

    if (useTimeouts || vote->IsRejected())
        StartPreparing();
    else
        EventLoop::Add(&proposeTimeout);
}

void PaxosProposer::OnRestartTimeout()
{
    Log_Debug("OnRestartTimeout");
    Log_Trace();

    ASSERT(!state.preparing);
    ASSERT(!state.proposing);
    ASSERT(!prepareTimeout.IsActive());
    ASSERT(!proposeTimeout.IsActive());

    if (context->IsPaxosBlocked())
    {
        EventLoop::Add(&restartTimeout);
        return;
    }

    if (useTimeouts)
        StartPreparing();
    else
        EventLoop::Add(&restartTimeout);
}

void PaxosProposer::Propose(Buffer& value)
{
    Log_Trace();
    
    if (IsActive())
        ASSERT_FAIL();

    state.proposedRunID = REPLICATION_CONFIG->GetRunID();
    ASSERT(value.GetLength() > 0);
    state.proposedValue.Write(value);

    if (state.multi && state.numProposals == 0)
    {
        state.numProposals++;
        StartProposing();
    }
    else
        StartPreparing();   
}

void PaxosProposer::Restart()
{
    Timer* timer;
    
    if (!state.preparing && !state.proposing)
        return;

    if (state.preparing)
        timer = &prepareTimeout;
    else
        timer = &proposeTimeout;
    
    EventLoop::Remove(timer);

    if (context->IsPaxosBlocked())
    {
        if (useTimeouts)
            EventLoop::Add(timer);
        return;
    }        

    StartPreparing();
}

void PaxosProposer::Stop()
{
    state.proposedRunID = 0;
    state.proposedValue.Clear();
    state.preparing = false;
    state.proposing = false;
    EventLoop::Remove(&prepareTimeout);
    EventLoop::Remove(&proposeTimeout);
    EventLoop::Remove(&restartTimeout);
}

bool PaxosProposer::IsActive()
{
    return (state.preparing || state.proposing || restartTimeout.IsActive());
}

uint64_t PaxosProposer::GetMemoryUsage()
{
    return sizeof(PaxosProposer) + state.proposedValue.GetSize();
}

void PaxosProposer::OnPrepareResponse(PaxosMessage& imsg)
{
    Log_Trace("msg.nodeID = %u", imsg.nodeID);

    if (!state.preparing || imsg.proposalID != state.proposalID)
        return;
    
    if (imsg.type == PAXOS_PREPARE_REJECTED)
        vote->RegisterRejected(imsg.nodeID);
    else
        vote->RegisterAccepted(imsg.nodeID);
    
    if (imsg.type == PAXOS_PREPARE_REJECTED)
    {
        Log_Debug("Prepare rejected");
        if (imsg.promisedProposalID > state.highestPromisedProposalID)
            state.highestPromisedProposalID = imsg.promisedProposalID;
    }
    else if (imsg.type == PAXOS_PREPARE_PREVIOUSLY_ACCEPTED &&
             imsg.acceptedProposalID >= state.highestReceivedProposalID)
    {
        /* the >= could be replaced by > which would result in less copys
         * however this would result in complications in multi paxos
         * in the multi paxos steady state this branch is inactive
         * it only runs after leader failure
         * so it's ok
         */
        state.highestReceivedProposalID = imsg.acceptedProposalID;
        state.proposedRunID = imsg.runID;
        ASSERT(imsg.value.GetLength() > 0);
        state.proposedValue.Write(imsg.value);
    }

    if (vote->IsRejected())
    {
        StopPreparing();
        EventLoop::Add(&restartTimeout);
    }
    else if (vote->IsAccepted())
        StartProposing();
}

void PaxosProposer::OnProposeResponse(PaxosMessage& imsg)
{
    PaxosMessage omsg;
    
    Log_Trace("msg.nodeID = %u", imsg.nodeID);
    
    if (!state.proposing || imsg.proposalID != state.proposalID)
        return;
    
    if (imsg.type == PAXOS_PROPOSE_REJECTED)
    {
        Log_Debug("Propose rejected");
        vote->RegisterRejected(imsg.nodeID);
    }
    else
        vote->RegisterAccepted(imsg.nodeID);

    if (vote->IsRejected())
    {
        StopProposing();
        EventLoop::Add(&restartTimeout);
    }
    else if (vote->IsAccepted())
    {
        // a majority have accepted our proposal, we have consensus
        StopProposing();
        omsg.LearnProposal(context->GetPaxosID(), MY_NODEID, state.proposalID);
        BroadcastMessage(omsg);
    }
}

void PaxosProposer::BroadcastMessage(PaxosMessage& omsg)
{
    Log_Trace();
    
    vote->Reset();
    context->GetTransport()->BroadcastMessage(omsg);
}

void PaxosProposer::StopPreparing()
{
    Log_Trace();

    state.preparing = false;
    EventLoop::Remove(&prepareTimeout);
}

void PaxosProposer::StopProposing()
{
    Log_Trace();
    
    state.proposing = false;
    EventLoop::Remove(&proposeTimeout);
}

void PaxosProposer::StartPreparing()
{
    PaxosMessage omsg;
    
    Log_Trace();

    StopProposing();

    NewVote();
    state.preparing = true;
    state.numProposals++;
    state.proposalID = REPLICATION_CONFIG->NextProposalID(MAX(state.proposalID, state.highestPromisedProposalID));
    state.highestReceivedProposalID = 0;
    
    omsg.PrepareRequest(context->GetPaxosID(), MY_NODEID, state.proposalID);
    BroadcastMessage(omsg);
    
    if (restartTimeout.IsActive())
        EventLoop::Remove(&restartTimeout);
    EventLoop::Reset(&prepareTimeout);
}

void PaxosProposer::StartProposing()
{
    PaxosMessage omsg;
    
    Log_Trace();
    
    StopPreparing();

    NewVote();
    state.proposing = true;
    
    ASSERT(state.proposedValue.GetLength() > 0);
    omsg.ProposeRequest(context->GetPaxosID(), MY_NODEID, state.proposalID,
     state.proposedRunID, state.proposedValue);
    BroadcastMessage(omsg);
    
    if (restartTimeout.IsActive())
        EventLoop::Remove(&restartTimeout);
    EventLoop::Reset(&proposeTimeout);
}

void PaxosProposer::NewVote()
{
    delete vote;
    vote = context->GetQuorum()->NewVote();
}
