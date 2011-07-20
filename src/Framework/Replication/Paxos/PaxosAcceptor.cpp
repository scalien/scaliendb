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
    ResetState();
}

void PaxosAcceptor::OnPrepareRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    senderID = imsg.nodeID;

    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);

    if (imsg.paxosID != context->GetPaxosID() ||
     imsg.proposalID < state.promisedProposalID ||
     context->GetDatabase()->IsCommiting() ||
     context->IsPaxosBlocked())
    {
        Log_Debug("imsg.paxosID = %U", imsg.paxosID);
        Log_Debug("context->GetPaxosID() = %U", context->GetPaxosID());
        Log_Debug("imsg.proposalID = %U", imsg.proposalID);
        Log_Debug("state.promisedProposalID = %U", state.promisedProposalID);
        Log_Debug("context->GetDatabase()->IsCommiting() = %b", context->GetDatabase()->IsCommiting());
        Log_Debug("context->IsPaxosBlocked() = %b", context->IsPaxosBlocked());
        omsg.PrepareRejected(imsg.paxosID, MY_NODEID,
         imsg.proposalID, state.promisedProposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.promisedProposalID = imsg.proposalID;

    if (!state.accepted)
    {
        omsg.PrepareCurrentlyOpen(imsg.paxosID, MY_NODEID, imsg.proposalID);
    }
    else
    {
        ASSERT(state.acceptedValue.GetLength() > 0);
        omsg.PreparePreviouslyAccepted(imsg.paxosID, MY_NODEID,
         imsg.proposalID, state.acceptedProposalID,
         state.acceptedRunID, state.acceptedValue);
    }
    
    WriteState();
    Commit(true);
}

void PaxosAcceptor::OnProposeRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    senderID = imsg.nodeID;

    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);
    
    if (imsg.paxosID != context->GetPaxosID() ||
     imsg.proposalID < state.promisedProposalID ||
     context->GetDatabase()->IsCommiting() ||
     context->IsPaxosBlocked())
    {
        omsg.ProposeRejected(imsg.paxosID, MY_NODEID, imsg.proposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.accepted = true;
    state.acceptedProposalID = imsg.proposalID;
    state.acceptedRunID = imsg.runID;
    ASSERT(imsg.value.GetLength() > 0);
    state.acceptedValue.Write(imsg.value);
    omsg.ProposeAccepted(imsg.paxosID, MY_NODEID, imsg.proposalID);
    
    WriteState();
    Commit(true);
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
        ASSERT(state.acceptedValue.GetLength() > 0);
    }    
}

void PaxosAcceptor::WriteState()
{
    QuorumDatabase* db;
    
    db = context->GetDatabase();
    
    db->SetPaxosID(context->GetPaxosID());
    db->SetAccepted(state.accepted);
    db->SetPromisedProposalID(state.promisedProposalID);
    if (state.accepted)
    {
        db->SetAcceptedRunID(state.acceptedRunID);
        db->SetAcceptedProposalID(state.acceptedProposalID);
        ASSERT(state.acceptedValue.GetLength() > 0);
        db->SetAcceptedValue(context->GetPaxosID(), state.acceptedValue);
    }
}

void PaxosAcceptor::Commit(bool sendReply_)
{
    QuorumDatabase* db;
    
    db = context->GetDatabase();
    
    sendReply = sendReply_; // used in OnStateWritten()
    
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

void PaxosAcceptor::ResetState()
{
    bool ac;
    
    state.Init();

    ac = asyncCommit;
    asyncCommit = false; // force sync commit
    WriteState();
    Commit(false);
    asyncCommit = ac;
}

uint64_t PaxosAcceptor::GetMemoryUsage()
{
    return sizeof(PaxosAcceptor) + state.acceptedValue.GetSize();
}
