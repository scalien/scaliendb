#include "ReplicatedLog.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "System/Events/EventLoop.h"

static Buffer enableMultiPaxos;

void ReplicatedLog::Init(QuorumContext* context_)
{
    Log_Trace();
    
    context = context_;

    paxosID = 0;
    
    proposer.Init(context);
    acceptor.Init(context);

    lastRequestChosenTime = 0;
    lastRequestChosenPaxosID = 0;
    
    enableMultiPaxos.Write("EnableMultiPaxos");
}

bool ReplicatedLog::IsMultiPaxosEnabled()
{
    return proposer.state.multi;
}

void ReplicatedLog::TryAppendNextValue()
{
    Log_Trace();
    
    if (!context->IsLeaseOwner() || proposer.IsActive() || !proposer.state.multi)
        return;
    
    Buffer& value = context->GetNextValue();
    if (value.GetLength() == 0)
        return;
    
    Append(value);
}

void ReplicatedLog::Append(Buffer& value)
{
    Log_Trace();
        
    if (proposer.IsActive())
        return;
    
    proposer.Propose(value);
}

void ReplicatedLog::SetPaxosID(uint64_t paxosID_)
{
    paxosID = paxosID_;
}

uint64_t ReplicatedLog::GetPaxosID()
{
    return paxosID;
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

void ReplicatedLog::OnPrepareRequest(PaxosMessage& imsg)
{
    Log_Trace();
        
    if (imsg.paxosID == paxosID)
        return acceptor.OnPrepareRequest(imsg);

    OnRequest(imsg);
}

void ReplicatedLog::OnPrepareResponse(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (imsg.paxosID == paxosID)
        proposer.OnPrepareResponse(imsg);
}

void ReplicatedLog::OnProposeRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (imsg.paxosID == paxosID)
        return acceptor.OnProposeRequest(imsg);
    
    OnRequest(imsg);
}

void ReplicatedLog::OnProposeResponse(PaxosMessage& imsg)
{
    Log_Trace();

    if (imsg.paxosID == paxosID)
        proposer.OnProposeResponse(imsg);
}

void ReplicatedLog::OnLearnChosen(PaxosMessage& imsg)
{
    uint64_t        runID;
    ReadBuffer      value;

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
}

void ReplicatedLog::ProcessLearnChosen(uint64_t nodeID, uint64_t runID, ReadBuffer value)
{
    bool ownAppend;

    Log_Trace("+++ Value for paxosID = %" PRIu64 ": %.*s +++", paxosID, P(&value));
    
    context->GetDatabase()->SetLearnedValue(paxosID, value);
    if (paxosID == (context->GetHighestPaxosID() - 1))
        context->GetDatabase()->Commit();

    NewPaxosRound(); // increments paxosID, clears proposer, acceptor
    
    if (context->GetHighestPaxosID() >= paxosID)
        RequestChosen(nodeID);
    
    ownAppend = proposer.state.multi;
    if (nodeID == REPLICATION_CONFIG->GetNodeID() && runID == REPLICATION_CONFIG->GetRunID() && context->IsLeaseOwner())
    {
        proposer.state.multi = true;
        Log_Trace("Multi paxos enabled");
    }
    else
    {
        proposer.state.multi = false;
        Log_Trace("Multi paxos disabled");
    }

    if (!BUFCMP(&value, &enableMultiPaxos))
        context->OnAppend(value, ownAppend);
    TryAppendNextValue();
}

void ReplicatedLog::OnRequestChosen(PaxosMessage& imsg)
{
    Buffer          value;
    PaxosMessage    omsg;
    
    Log_Trace();

    if (imsg.paxosID >= GetPaxosID())
        return;
    
    // the node is lagging and needs to catch-up
    context->GetDatabase()->GetLearnedValue(imsg.paxosID, value);
    if (value.GetLength() > 0)
    {
        Log_Trace("Sending paxosID %d to node %d", imsg.paxosID, imsg.nodeID);
        omsg.LearnValue(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(), 0, value);
    }
    else
    {
        Log_Trace("Node requested a paxosID I no longer have");
        omsg.StartCatchup(paxosID, REPLICATION_CONFIG->GetNodeID());
    }
    context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}

void ReplicatedLog::OnStartCatchup(PaxosMessage& imsg)
{
    if (imsg.nodeID == context->GetLeaseOwner())
        context->OnStartCatchup();
}

void ReplicatedLog::OnRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    Buffer          value;
    PaxosMessage    omsg;

    if (imsg.paxosID < GetPaxosID())
    {
        // the node is lagging and needs to catch-up
        context->GetDatabase()->GetLearnedValue(imsg.paxosID, value);
        if (value.GetLength() == 0)
            return;
        omsg.LearnValue(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(), 0, value);
        context->GetTransport()->SendMessage(imsg.nodeID, omsg);
    }
    else // paxosID < msg.paxosID
    {
        //  I am lagging and need to catch-up
        RequestChosen(imsg.nodeID);
    }
}

void ReplicatedLog::NewPaxosRound()
{
    EventLoop::Remove(&(proposer.prepareTimeout));
    EventLoop::Remove(&(proposer.proposeTimeout));

    paxosID++;
    proposer.state.OnNewPaxosRound();
    acceptor.state.OnNewPaxosRound();
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
        Log_Trace("Appending EnableMultiPaxos");
        Append(enableMultiPaxos);
    }
}

void ReplicatedLog::OnLeaseTimeout()
{
    proposer.state.multi = false;
}

bool ReplicatedLog::IsAppending()
{
    return context->IsLeaseOwner() && proposer.state.numProposals > 0;
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

void ReplicatedLog::RequestChosen(uint64_t nodeID)
{
    PaxosMessage omsg;
    
    Log_Trace();
    
    if (lastRequestChosenPaxosID == GetPaxosID() &&
     EventLoop::Now() - lastRequestChosenTime < REQUEST_CHOSEN_TIMEOUT)
        return;
    
    lastRequestChosenPaxosID = GetPaxosID();
    lastRequestChosenTime = EventLoop::Now();
    
    omsg.RequestChosen(GetPaxosID(), REPLICATION_CONFIG->GetNodeID());
    
    context->GetTransport()->SendMessage(nodeID, omsg);
}
