#include "ReplicatedLog.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "System/Events/EventLoop.h"

//#define RLOG_DEBUG_MESSAGES 1

static Buffer dummy;

ReplicatedLog::ReplicatedLog()
{
    canaryTimer.SetCallable(MFUNC(ReplicatedLog, OnCanaryTimeout));
    canaryTimer.SetDelay(CANARY_TIMEOUT);
}

void ReplicatedLog::Init(QuorumContext* context_)
{
    Log_Trace();
    
    context = context_;

    paxosID = 0;
    waitingOnAppend = false;
    appendDummyNext = false;
    
    proposer.Init(context);
    acceptor.Init(context);

    lastRequestChosenTime = 0;
    lastLearnChosenTime = 0;
    replicationThroughput = 0;

    EventLoop::Add(&canaryTimer);
    
    dummy.Write("dummy");
}

void ReplicatedLog::Shutdown()
{
    EventLoop::Remove(&canaryTimer);
    proposer.RemoveTimers();
}

uint64_t ReplicatedLog::GetLastLearnChosenTime()
{
    return lastLearnChosenTime;
}

uint64_t ReplicatedLog::GetReplicationThroughput()
{
    return replicationThroughput;
}

void ReplicatedLog::Stop()
{
    proposer.Stop();
}

void ReplicatedLog::Continue()
{
    if (context->IsLeaseKnown() && context->GetHighestPaxosID() > GetPaxosID())
        RequestChosen(context->GetLeaseOwner());
}

bool ReplicatedLog::IsMultiPaxosEnabled()
{
    return proposer.state.multi;
}

bool ReplicatedLog::IsAppending()
{
    return context->IsLeaseOwner() && proposer.state.numProposals > 0;
}

bool ReplicatedLog::IsWaitingOnAppend()
{
    return waitingOnAppend;
}

void ReplicatedLog::TryAppendDummy()
{
    Log_Trace();
    
    proposer.SetUseTimeouts(true);
    
    if (proposer.IsActive() || proposer.IsLearnSent())
    {
        appendDummyNext = true;
        return;
    }

    if (waitingOnAppend)
    {
        appendDummyNext = true;
        Log_Message("TryAppendDummy called, but waitingOnAppend = true");
        return;
    }

    Append(dummy);
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("Appending DUMMY!");
#endif
}

void ReplicatedLog::TryAppendNextValue()
{
    Log_Trace();
    
    if (waitingOnAppend)
        return;

    if (!context->IsLeaseOwner() || proposer.IsActive() || proposer.IsLearnSent() || !proposer.state.multi)
        return;

    if (appendDummyNext)
    {
        appendDummyNext = false;
        TryAppendDummy();
        return;
    }
    
    Buffer& value = context->GetNextValue();
    if (value.GetLength() == 0)
        return;
    
    proposer.SetUseTimeouts(context->UseProposeTimeouts());
    Append(value);
}

void ReplicatedLog::TryCatchup()
{
    if (context->IsLeaseKnown() && context->GetHighestPaxosID() > GetPaxosID())
        RequestChosen(context->GetLeaseOwner());
}

void ReplicatedLog::Restart()
{
    if (waitingOnAppend)
        return;

    if (proposer.IsLearnSent())
        return;

    context->OnStartProposing();

    proposer.state.multi = false;
    if (proposer.IsActive())
        proposer.Restart();
}

void ReplicatedLog::SetPaxosID(uint64_t paxosID_)
{
    paxosID = paxosID_;
}

uint64_t ReplicatedLog::GetPaxosID()
{
    return paxosID;
}

void ReplicatedLog::NewPaxosRound()
{
    paxosID++;
    proposer.RemoveTimers();
    proposer.state.OnNewPaxosRound();
    acceptor.state.OnNewPaxosRound();
    lastRequestChosenTime = 0;
}

void ReplicatedLog::RegisterPaxosID(uint64_t paxosID, uint64_t nodeID)
{
    Log_Trace();
    
    if (paxosID > GetPaxosID())
    {
        //  I am lagging and need to catch-up
        RequestChosen(nodeID);
    }
}

void ReplicatedLog::OnMessage(PaxosMessage& imsg)
{
    Log_Trace();
    bool processed;

    processed = false;
    if (imsg.type == PAXOS_PREPARE_REQUEST)
        processed = OnPrepareRequest(imsg);
    else if (imsg.IsPrepareResponse())
        processed = OnPrepareResponse(imsg);
    else if (imsg.type == PAXOS_PROPOSE_REQUEST)
        processed = OnProposeRequest(imsg);
    else if (imsg.IsProposeResponse())
        processed = OnProposeResponse(imsg);
    else if (imsg.IsLearn())
        processed = OnLearnChosen(imsg);
    else if (imsg.type == PAXOS_REQUEST_CHOSEN)
        processed = OnRequestChosen(imsg);
    else if (imsg.type == PAXOS_START_CATCHUP)
        processed = OnStartCatchup(imsg);
    else
        ASSERT_FAIL();

    if (processed)
        context->OnMessageProcessed();
}

void ReplicatedLog::OnCatchupStarted()
{
    acceptor.OnCatchupStarted();
}

void ReplicatedLog::OnCatchupComplete(uint64_t paxosID_)
{
    paxosID = paxosID_;
    
    acceptor.OnCatchupComplete(); // commits
    
    NewPaxosRound();
}

void ReplicatedLog::OnLearnLease()
{
    Log_Trace("context->IsLeaseOwner()   = %s", (context->IsLeaseOwner() ? "true" : "false"));
    Log_Trace("!proposer.IsActive()  = %s", (!proposer.IsActive() ? "true" : "false"));
    Log_Trace("!proposer.state.multi = %s", (!proposer.state.multi ? "true" : "false"));
    if (context->IsLeaseOwner() && !proposer.IsActive() && !proposer.state.multi)
    {
        Log_Trace("Appending dummy to enable MultiPaxos");
        TryAppendDummy();
    }
}

void ReplicatedLog::OnLeaseTimeout()
{
    proposer.Stop();
}

void ReplicatedLog::OnAppendComplete()
{
    waitingOnAppend = false;

    NewPaxosRound(); // increments paxosID, clears proposer, acceptor
    
    if (context->IsLeaseKnown() && paxosID <= context->GetHighestPaxosID())
        RequestChosen(context->GetLeaseOwner());

    if (paxosID < context->GetHighestPaxosID())
        context->GetDatabase()->Commit();

    if (!context->UseCommitChaining())
    {
        acceptor.WriteState();
        context->GetDatabase()->Commit();
    }

    context->OnMessageProcessed();

    TryAppendNextValue();
}

void ReplicatedLog::WriteState()
{
    acceptor.WriteState();
}

uint64_t ReplicatedLog::GetMemoryUsage()
{
    return sizeof(ReplicatedLog) - 
        sizeof(PaxosAcceptor) + acceptor.GetMemoryUsage() -
        sizeof(PaxosProposer) + proposer.GetMemoryUsage();
}

void ReplicatedLog::Append(Buffer& value)
{
    Log_Trace();
        
    if (proposer.IsActive() || proposer.IsLearnSent())
        return;
    
    context->OnStartProposing();
    proposer.Propose(value);
    
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("Proposing for paxosID = %U", GetPaxosID());
#endif
}

bool ReplicatedLog::OnPrepareRequest(PaxosMessage& imsg)
{
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnPrepareRequest");
#endif

    bool processed = acceptor.OnPrepareRequest(imsg);

    OnRequest(imsg);

    return processed;
}

bool ReplicatedLog::OnPrepareResponse(PaxosMessage& imsg)
{
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnPrepareResponse");
#endif

    Log_Trace();
    
    if (imsg.paxosID == paxosID)
        proposer.OnPrepareResponse(imsg);
    
    return true;
}

bool ReplicatedLog::OnProposeRequest(PaxosMessage& imsg)
{
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnProposeRequest");
#endif

    Log_Trace();
    
    bool processed = acceptor.OnProposeRequest(imsg);
    
    OnRequest(imsg);

    return processed;
}

bool ReplicatedLog::OnProposeResponse(PaxosMessage& imsg)
{
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnProposeResponse");
#endif

    Log_Trace();

    if (imsg.paxosID == paxosID)
        proposer.OnProposeResponse(imsg);

    return true;
}

bool ReplicatedLog::OnLearnChosen(PaxosMessage& imsg)
{
    uint64_t        runID;

#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("OnLearnChosen begin");
#endif

    if (context->GetDatabase()->IsCommitting())
    {
#ifdef RLOG_DEBUG_MESSAGES
        Log_Debug("Database is commiting, dropping Paxos message");
#endif
        return true;
    }

    if (waitingOnAppend)
    {
#ifdef RLOG_DEBUG_MESSAGES
        Log_Debug("Waiting OnAppend, dropping Paxos message");
#endif
        return true;
    }

    // if I was the primary, lost the lease in the middle of round 1000, another node replicated some rounds 1001-1100,
    // I get the lease back, and then receive the learn message for my previous round (1000)
    // then multi will be turned back on, but then subsequent rounds (1001-1100), where I will
    // receive learn messages will not succeed, and due to the code above
    // I would throw it away any learn messages sent to me

    //if (imsg.nodeID != MY_NODEID && proposer.state.multi)
    //{
    //    Log_Debug("Received learn message from %U, but I'm in multi paxos mode", imsg.nodeID);
    //    return true;
    //}

    // it's valid for me to be the primary and be lagging by one round * at the beginning of my lease *
    // this can happen if the old primary completes a round of replication, fails
    // and I get the lease before its OnLearnChosen arrives
    // if I throw away the OnLearnChosen, but the others in the quorum do not
    // they will advance their paxosID, I will not advance mine
    // and I will not be able to replicate => read-only cluster

    //if (imsg.nodeID != MY_NODEID && context->IsLeaseOwner())
    //{
    //    Log_Debug("Received learn message from %U, but I'm the lease owner", imsg.nodeID);
    //    return true;
    //}

    Log_Trace();

    if (imsg.paxosID > paxosID)
    {
        RequestChosen(imsg.nodeID); //  I am lagging and need to catch-up
        return true;
    }
    else if (imsg.paxosID < paxosID)
        return true;
    
    if (imsg.type == PAXOS_LEARN_VALUE)
    {
        runID = 0;
        // in the PAXOS_LEARN_VALUE case (and only in this case) runID is 0
        // for legacy reasons the PAXOS_LEARN_VALUE message also includes a runID,
        // which is always set to 0
        acceptor.state.accepted = true;
        acceptor.state.acceptedValue.Write(imsg.value);
        acceptor.WriteState();
    }
    else if (imsg.type == PAXOS_LEARN_PROPOSAL && acceptor.state.accepted &&
     acceptor.state.acceptedProposalID == imsg.proposalID)
    {
        runID = acceptor.state.acceptedRunID;
    }
    else
    {
        RequestChosen(imsg.nodeID);
        return true;
    }
    
    ProcessLearnChosen(imsg.nodeID, runID);

#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("OnLearnChosen end");
#endif

    return false;
}

bool ReplicatedLog::OnRequestChosen(PaxosMessage& imsg)
{
    Buffer          value;
    PaxosMessage    omsg;
    
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnRequestChosen, imsg.paxosID = %U, mine = %U",
     imsg.paxosID, GetPaxosID());
#endif

    if (imsg.paxosID >= GetPaxosID())
        return true;
    
    // the node is lagging and needs to catch-up

    if (context->AlwaysUseDatabaseCatchup() && imsg.paxosID < GetPaxosID())
    {
        omsg.StartCatchup(paxosID, MY_NODEID);
    }
    else
    {
        context->GetDatabase()->GetAcceptedValue(imsg.paxosID, value);
        if (value.GetLength() > 0)
        {
            Log_Trace("Sending paxosID %d to node %d", imsg.paxosID, imsg.nodeID);
            omsg.LearnValue(imsg.paxosID, MY_NODEID, 0, value);
        }
        else
        {
            Log_Trace("Node requested a paxosID I no longer have");
            omsg.StartCatchup(paxosID, MY_NODEID);
        }
    }
    context->GetTransport()->SendMessage(imsg.nodeID, omsg);

    return true;
}

bool ReplicatedLog::OnStartCatchup(PaxosMessage& imsg)
{
#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::OnStartCatchup");
#endif

    if (imsg.nodeID == context->GetLeaseOwner())
        context->OnStartCatchup();

    return true;
}

void ReplicatedLog::OnCanaryTimeout()
{
    if (context->IsLeaseOwner() && !IsAppending())
        TryAppendDummy();

    EventLoop::Add(&canaryTimer);
}

void ReplicatedLog::ProcessLearnChosen(uint64_t nodeID, uint64_t runID)
{
    bool        ownAppend;
    uint64_t    now;
    Buffer      learnedValue;

    learnedValue.Write(acceptor.state.acceptedValue);

    if (lastLearnChosenTime > 0)
    {
        now = EventLoop::Now();
        if (now > lastLearnChosenTime)
        {
            // byte / msec = kbyte / sec
            replicationThroughput = learnedValue.GetLength() / (now - lastLearnChosenTime);
            // convert to byte / sec
            replicationThroughput *= 1000;
        }
    }

    lastLearnChosenTime = EventLoop::Now();

#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("Round completed for paxosID = %U", paxosID);
    Log_Trace("+++ Value for paxosID = %U: %B +++", paxosID, &learnedValue);
    if (context->GetHighestPaxosID() > 0 && paxosID < context->GetHighestPaxosID())
    {
        Log_Debug("Paxos-based catchup, highest seen paxosID is %U, currently at %U",
         context->GetHighestPaxosID(), paxosID);
        if (paxosID == (context->GetHighestPaxosID() - 1))
            Log_Debug("Paxos-based catchup complete...");
    }
#endif
    
    if (context->GetHighestPaxosID() > 0 && paxosID < (context->GetHighestPaxosID() - 1))
        context->GetDatabase()->Commit();
    
    waitingOnAppend = true;
    ownAppend = proposer.state.multi;
    if (nodeID == MY_NODEID && runID == REPLICATION_CONFIG->GetRunID() && context->IsLeaseOwner())
    {
        proposer.state.multi = true;
        if (!ownAppend)
            context->OnIsLeader();
        Log_Trace("Multi paxos enabled");
    }
    else
    {
        proposer.state.multi = false;
        Log_Trace("Multi paxos disabled");
    }

    ownAppend &= proposer.state.multi;

    if (BUFCMP(&learnedValue, &dummy))
        OnAppendComplete();
    else
    {
        context->OnAppend(paxosID, learnedValue, ownAppend);
        // QuorumContext::OnAppend() must call ReplicatedLog::OnAppendComplete()
    }
}

void ReplicatedLog::OnRequest(PaxosMessage& imsg)
{
    Buffer          value;
    PaxosMessage    omsg;

    Log_Trace();

    if (imsg.paxosID < GetPaxosID())
    {
        context->GetDatabase()->GetAcceptedValue(imsg.paxosID, value);
        if (value.GetLength() == 0)
            return;
        omsg.LearnValue(imsg.paxosID, MY_NODEID, 0, value);
        context->GetTransport()->SendMessage(imsg.nodeID, omsg);
    }
    else if (GetPaxosID() < imsg.paxosID)
    {
        //  I am lagging and need to catch-up
        RequestChosen(imsg.nodeID);
    }
}

void ReplicatedLog::RequestChosen(uint64_t nodeID)
{
    PaxosMessage omsg;
    
    if (context->IsLeaseOwner())
        return;
    if (EventLoop::Now() - lastRequestChosenTime < REQUEST_CHOSEN_TIMEOUT)
        return;
    if (waitingOnAppend)
        return;

    lastRequestChosenTime = EventLoop::Now();
    
    omsg.RequestChosen(GetPaxosID(), MY_NODEID);
    
    context->GetTransport()->SendMessage(nodeID, omsg);

#ifdef RLOG_DEBUG_MESSAGES
    Log_Debug("ReplicatedLog::RequestChosen, paxosID = %U, to = %U", GetPaxosID(), nodeID);
#endif
}
