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
    isCommitting = false;
    
    ReadState();
}

bool PaxosAcceptor::OnPrepareRequest(PaxosMessage& imsg)
{
    bool reject;

    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);

    reject = TestRejection(imsg);

    if (reject)
    {
        omsg.PrepareRejected(imsg.paxosID, MY_NODEID,
         imsg.proposalID, state.promisedProposalID);
        context->GetTransport()->SendMessage(imsg.nodeID, omsg);
        return true; // msg processed
    }

    AcceptPrepareRequest(imsg);
    return false; // OnMessageProcessed() will be called in OnStateWritten()
}

bool PaxosAcceptor::OnProposeRequest(PaxosMessage& imsg)
{
    bool reject;

    Log_Trace("state.promisedProposalID: %U msg.proposalID: %U",
     state.promisedProposalID, imsg.proposalID);
    
    reject = TestRejection(imsg);

    if (reject)
    {
        omsg.ProposeRejected(imsg.paxosID, MY_NODEID, imsg.proposalID);
        context->GetTransport()->SendMessage(imsg.nodeID, omsg);
        return true; // msg processed
    }

    AcceptProposeRequest(imsg);
    return false; // OnMessageProcessed() will be called in OnStateWritten()
}

void PaxosAcceptor::OnCatchupComplete()
{
    ASSERT(!isCommitting);
    ASSERT(!context->GetDatabase()->IsCommiting());

    state.Init();
    WriteState();
    context->GetDatabase()->Commit();
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

uint64_t PaxosAcceptor::GetMemoryUsage()
{
    return sizeof(PaxosAcceptor) + state.acceptedValue.GetSize();
}

void PaxosAcceptor::Commit()
{
    ASSERT(!isCommitting);

    writtenPaxosID = context->GetPaxosID();
    isCommitting = true; // reset in OnStateWritten()

    if (context->UseSyncCommit())
    {
        context->GetDatabase()->Commit();
        OnStateWritten();
    }
    else
    {
        context->GetDatabase()->Commit(onStateWritten);
    }
}

void PaxosAcceptor::OnStateWritten()
{
    Log_Trace();
    
    isCommitting = false;

    context->OnMessageProcessed();

    if (writtenPaxosID == context->GetPaxosID())
        context->GetTransport()->SendMessage(senderID, omsg);
}

void PaxosAcceptor::ReadState()
{
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

bool PaxosAcceptor::TestRejection(PaxosMessage& msg)
{
    bool reject;

    reject = false;
    if (msg.paxosID != context->GetPaxosID())
        reject = true;
    if (msg.proposalID < state.promisedProposalID)
        reject = true;
    if (isCommitting)
        reject = true;
    if (context->GetDatabase()->IsCommiting())
        reject = true;
    if (context->IsPaxosBlocked())
        reject = true;

    if (reject)
    {
        Log_Debug("imsg.paxosID = %U", msg.paxosID);
        Log_Debug("context->GetPaxosID() = %U", context->GetPaxosID());
        Log_Debug("imsg.proposalID = %U", msg.proposalID);
        Log_Debug("state.promisedProposalID = %U", state.promisedProposalID);
        Log_Debug("context->GetDatabase()->IsCommiting() = %b", context->GetDatabase()->IsCommiting());
        Log_Debug("context->IsPaxosBlocked() = %b", context->IsPaxosBlocked());
    }

    return reject;
}

void PaxosAcceptor::AcceptPrepareRequest(PaxosMessage& imsg)
{
    state.promisedProposalID = imsg.proposalID;

    senderID = imsg.nodeID;
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
    Commit();
}

void PaxosAcceptor::AcceptProposeRequest(PaxosMessage& imsg)
{
    state.accepted = true;
    state.acceptedProposalID = imsg.proposalID;
    state.acceptedRunID = imsg.runID;
    ASSERT(imsg.value.GetLength() > 0);
    state.acceptedValue.Write(imsg.value);

    senderID = imsg.nodeID;
    omsg.ProposeAccepted(imsg.paxosID, MY_NODEID, imsg.proposalID);
    
    WriteState();
    Commit();
}
