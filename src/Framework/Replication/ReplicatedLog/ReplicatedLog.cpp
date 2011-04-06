#include "ReplicatedLog.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "System/Events/EventLoop.h"

static Buffer dummy;

void ReplicatedLog::Init(QuorumContext* context_)
{
    Log_Trace();
    
    context = context_;

    paxosID = 0;
    
    proposer.Init(context);
    acceptor.Init(context);

    lastRequestChosenTime = 0;
    lastRequestChosenPaxosID = 0;
    commitChaining = false;
    
    dummy.Write("dummy");
}

void ReplicatedLog::Shutdown()
{
    proposer.Shutdown();
}

void ReplicatedLog::SetUseProposeTimeouts(bool useProposeTimeouts_)
{
    useProposeTimeouts = useProposeTimeouts_;
}

void ReplicatedLog::SetCommitChaining(bool commitChaining_)
{
    commitChaining = commitChaining_;
}

bool ReplicatedLog::GetCommitChaining()
{
    return commitChaining;
}

void ReplicatedLog::SetAsyncCommit(bool asyncCommit)
{
    acceptor.SetAsyncCommit(asyncCommit);
}

bool ReplicatedLog::GetAsyncCommit()
{
    return acceptor.GetAsyncCommit();
}

void ReplicatedLog::Stop()
{
    proposer.Stop();
}

void ReplicatedLog::Continue()
{
    RequestChosen(paxosID);
}

bool ReplicatedLog::IsMultiPaxosEnabled()
{
    return proposer.state.multi;
}

bool ReplicatedLog::IsAppending()
{
    return context->IsLeaseOwner() && proposer.state.numProposals > 0;
}

void ReplicatedLog::TryAppendDummy()
{
    Log_Trace();
    
    if (proposer.IsActive())
        return;

    proposer.SetUseTimeouts(true);
    Append(dummy);
    Log_Trace("Appending DUMMY!");
}

void ReplicatedLog::TryAppendNextValue()
{
    Log_Trace();
    
    if (!context->IsLeaseOwner() || proposer.IsActive() || !proposer.state.multi)
        return;
    
    Buffer& value = context->GetNextValue();
    if (value.GetLength() == 0)
        return;
    
    proposer.SetUseTimeouts(useProposeTimeouts);
    Append(value);
}

void ReplicatedLog::TryCatchup()
{
    if (context->IsLeaseKnown())
        RequestChosen(context->GetLeaseOwner());
}

void ReplicatedLog::Restart()
{
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
    EventLoop::Remove(&(proposer.prepareTimeout));
    EventLoop::Remove(&(proposer.proposeTimeout));

    paxosID++;
    proposer.state.OnNewPaxosRound();
    acceptor.state.OnNewPaxosRound();
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
    
    if (imsg.type == PAXOS_PREPARE_REQUEST)
        OnPrepareRequest(imsg);
    else if (imsg.IsPrepareResponse())
        OnPrepareResponse(imsg);
    else if (imsg.type == PAXOS_PROPOSE_REQUEST)
        OnProposeRequest(imsg);
    else if (imsg.IsProposeResponse())
        OnProposeResponse(imsg);
    else if (imsg.IsLearn())
        OnLearnChosen(imsg);
    else if (imsg.type == PAXOS_REQUEST_CHOSEN)
        OnRequestChosen(imsg);
    else if (imsg.type == PAXOS_START_CATCHUP)
        OnStartCatchup(imsg);
    else
        ASSERT_FAIL();
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
    proposer.state.multi = false;
}

void ReplicatedLog::OnAppendComplete()
{
    // TODO: this is not a complete solution
    if (paxosID < context->GetHighestPaxosID())
        context->GetDatabase()->Commit();

    if (!commitChaining)
    {
        acceptor.WriteState();
        context->GetDatabase()->Commit();
    }

    TryAppendNextValue();
}

void ReplicatedLog::WriteState()
{
    acceptor.WriteState();
}

void ReplicatedLog::Append(Buffer& value)
{
    Log_Trace();
        
    if (proposer.IsActive())
        return;
    
    proposer.Propose(value);
    
//    Log_Debug("Proposing for paxosID = %U", GetPaxosID());
}

void ReplicatedLog::OnPrepareRequest(PaxosMessage& imsg)
{
//    Log_Debug("ReplicatedLog::OnPrepareRequest");

    acceptor.OnPrepareRequest(imsg);

    OnRequest(imsg);
}

void ReplicatedLog::OnPrepareResponse(PaxosMessage& imsg)
{
//    Log_Debug("ReplicatedLog::OnPrepareResponse");

    Log_Trace();
    
    if (imsg.paxosID == paxosID)
        proposer.OnPrepareResponse(imsg);
}

void ReplicatedLog::OnProposeRequest(PaxosMessage& imsg)
{
//    Log_Debug("ReplicatedLog::OnProposeRequest");

    Log_Trace();
    
    acceptor.OnProposeRequest(imsg);
    
    OnRequest(imsg);
}

void ReplicatedLog::OnProposeResponse(PaxosMessage& imsg)
{
//    Log_Debug("ReplicatedLog::OnProposeResponse");

    Log_Trace();

    if (imsg.paxosID == paxosID)
        proposer.OnProposeResponse(imsg);
}

void ReplicatedLog::OnLearnChosen(PaxosMessage& imsg)
{
    uint64_t        runID;
    ReadBuffer      value;

//    Log_Debug("OnLearnChosen begin");

    if (context->GetDatabase()->IsCommiting())
    {
//        Log_Debug("Database is commiting, dropping Paxos message");
        return;
    }

    Log_Trace();

    if (imsg.paxosID > paxosID)
    {
        RequestChosen(imsg.nodeID); //  I am lagging and need to catch-up
        return;
    }
    else if (imsg.paxosID < paxosID)
        return;
    
    if (imsg.type == PAXOS_LEARN_VALUE)
    {
        runID = imsg.runID;
        value = imsg.value;
        context->GetDatabase()->SetAcceptedValue(paxosID, value);
    }
    else if (imsg.type == PAXOS_LEARN_PROPOSAL && acceptor.state.accepted &&
     acceptor.state.acceptedProposalID == imsg.proposalID)
    {
        runID = acceptor.state.acceptedRunID;
        value.Wrap(acceptor.state.acceptedValue);
    }
    else
    {
        RequestChosen(imsg.nodeID);
        return;
    }
        
    ProcessLearnChosen(imsg.nodeID, runID, value);

//    Log_Debug("OnLearnChosen end");
}

void ReplicatedLog::OnRequestChosen(PaxosMessage& imsg)
{
    Buffer          value;
    PaxosMessage    omsg;
    
//    Log_Debug("ReplicatedLog::OnRequestChosen, imsg.paxosID = %U, mine = %U",
//     imsg.paxosID, GetPaxosID());

    if (imsg.paxosID >= GetPaxosID())
        return;
    
    // the node is lagging and needs to catch-up
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
    context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}

void ReplicatedLog::OnStartCatchup(PaxosMessage& imsg)
{
//    Log_Debug("ReplicatedLog::OnStartCatchup");

    if (imsg.nodeID == context->GetLeaseOwner())
        context->OnStartCatchup();
}

void ReplicatedLog::ProcessLearnChosen(uint64_t nodeID, uint64_t runID, ReadBuffer value)
{
    bool ownAppend;

//    Log_Debug("Round completed for paxosID = %U", paxosID);

    Log_Trace("+++ Value for paxosID = %U: %R +++", paxosID, &value);

    if (context->GetHighestPaxosID() > 0 && paxosID < context->GetHighestPaxosID())
    {
        Log_Debug("Paxos-based catchup, highest seen paxosID is %U, currently at %U",
         context->GetHighestPaxosID(), paxosID);
        if (paxosID == (context->GetHighestPaxosID() - 1))
            Log_Debug("Paxos-based catchup complete...");
    }
    
    if (context->GetHighestPaxosID() > 0 && paxosID < (context->GetHighestPaxosID() - 1))
        context->GetDatabase()->Commit();

    NewPaxosRound(); // increments paxosID, clears proposer, acceptor
    
    if (paxosID <= context->GetHighestPaxosID())
        RequestChosen(nodeID);
    
    ownAppend = proposer.state.multi;
    if (nodeID == MY_NODEID && runID == REPLICATION_CONFIG->GetRunID() && context->IsLeaseOwner())
    {
        if (!proposer.state.multi)
        {
            proposer.state.multi = true;
            context->OnIsLeader();
        }
        proposer.state.multi = true;
        Log_Trace("Multi paxos enabled");
    }
    else
    {
        proposer.state.multi = false;
        Log_Trace("Multi paxos disabled");
    }

    if (BUFCMP(&value, &dummy))
        OnAppendComplete();
    else
        context->OnAppend(paxosID - 1, value, ownAppend);

    // new convention: QuorumContext::OnAppend() must call
    // ReplicatedLog::OnAppendComplete()
    // when it's done!
}

void ReplicatedLog::OnRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    Buffer          value;
    PaxosMessage    omsg;

    if (imsg.paxosID < GetPaxosID())
    {
        // the node is lagging and needs to catch-up
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
    
    if (EventLoop::Now() - lastRequestChosenTime < REQUEST_CHOSEN_TIMEOUT)
        return;

    lastRequestChosenPaxosID = GetPaxosID();
    lastRequestChosenTime = EventLoop::Now();
    
    omsg.RequestChosen(GetPaxosID(), MY_NODEID);
    
    context->GetTransport()->SendMessage(nodeID, omsg);

//    Log_Debug("ReplicatedLog::RequestChosen, paxosID = %U, to = %U", GetPaxosID(), nodeID);
}
