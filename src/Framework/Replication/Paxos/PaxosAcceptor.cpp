#include "PaxosAcceptor.h"
#include "PaxosProposer.h"
#include "Framework/Replication/ReplicationConfig.h"

PaxosAcceptor::PaxosAcceptor()
{
    onStateWritten = MFUNC(PaxosAcceptor, OnStateWritten);
}

void PaxosAcceptor::Init(QuorumContext* context_)
{
    context = context_;
    sendReply = false;
    
    ReadState();
}

void PaxosAcceptor::SetAsyncCommit(bool asyncCommit_)
{
    asyncCommit = asyncCommit_;
}

bool PaxosAcceptor::GetAsyncCommit()
{
    return asyncCommit;
}

void PaxosAcceptor::OnMessage(PaxosMessage& imsg)
{
    if (imsg.type == PAXOS_PREPARE_REQUEST)
        OnPrepareRequest(imsg);
    else if (imsg.type == PAXOS_PROPOSE_REQUEST)
        OnProposeRequest(imsg);
    else
        ASSERT_FAIL();
}

void PaxosAcceptor::OnCatchupComplete()
{
    bool ac;
    
    state.Init();

    ac = asyncCommit;
    asyncCommit = false; // force sync commit on catchup complete
    WriteState(false);
    asyncCommit = ac;
}

void PaxosAcceptor::OnPrepareRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (context->GetDatabase()->IsActive())
        return;

    senderID = imsg.nodeID;
    
    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);
    
    if (imsg.proposalID < state.promisedProposalID)
    {
        omsg.PrepareRejected(imsg.paxosID, MY_NODEID,
         imsg.proposalID, state.promisedProposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.promisedProposalID = imsg.proposalID;

    if (!state.accepted)
        omsg.PrepareCurrentlyOpen(imsg.paxosID, MY_NODEID, imsg.proposalID);
    else
        omsg.PreparePreviouslyAccepted(imsg.paxosID, MY_NODEID,
         imsg.proposalID, state.acceptedProposalID,
         state.acceptedRunID, state.acceptedValue);
    
    WriteState(true);
}

void PaxosAcceptor::OnProposeRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (context->GetDatabase()->IsActive())
        return;
    
    senderID = imsg.nodeID;

    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);
    
    if (imsg.proposalID < state.promisedProposalID)
    {
        omsg.ProposeRejected(imsg.paxosID, MY_NODEID, imsg.proposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.accepted = true;
    state.acceptedProposalID = imsg.proposalID;
    state.acceptedRunID = imsg.runID;
    state.acceptedValue.Write(imsg.value);
    omsg.ProposeAccepted(imsg.paxosID, MY_NODEID, imsg.proposalID);
    
    WriteState(true);
}

void PaxosAcceptor::OnStateWritten()
{
    Log_Trace();
    
    if (!sendReply || writtenPaxosID != context->GetPaxosID())
        return;
    
    context->GetTransport()->SendMessage(senderID, omsg);
}

void PaxosAcceptor::ReadState()
{
    Log_Trace();
    
    QuorumDatabase* db;
    
    db = context->GetDatabase();

    context->SetPaxosID(db->GetPaxosID());
    state.accepted = db->GetAccepted();
    state.promisedProposalID = db->GetPromisedProposalID();
    if (state.accepted)
    {
        state.acceptedRunID = db->GetAcceptedRunID();
        state.acceptedProposalID = db->GetAcceptedProposalID();
        db->GetAcceptedValue(context->GetPaxosID(), state.acceptedValue);
    }
}

void PaxosAcceptor::WriteState(bool sendReply_)
{
    QuorumDatabase* db;
    
    sendReply = sendReply_; // used in OnStateWritten()
    
    db = context->GetDatabase();
    
    db->SetPaxosID(context->GetPaxosID());
    db->SetAccepted(state.accepted);
    db->SetPromisedProposalID(state.promisedProposalID);
    if (state.accepted)
    {
        db->SetAcceptedRunID(state.acceptedRunID);
        db->SetAcceptedProposalID(state.acceptedProposalID);
        db->SetAcceptedValue(context->GetPaxosID(), state.acceptedValue);
    }

    writtenPaxosID = context->GetPaxosID();
    
    if (asyncCommit)
    {
        db->Commit(onStateWritten);
    }
    else
    {
        db->Commit();
        OnStateWritten();
    }
}
