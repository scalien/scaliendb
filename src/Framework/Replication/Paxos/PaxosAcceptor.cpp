#include "PaxosAcceptor.h"
#include "PaxosProposer.h"
#include "Framework/Replication/ReplicationConfig.h"

void PaxosAcceptor::Init(QuorumContext* context_)
{
    context = context_;

    ReadState();
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
    state.Init();
    WriteState();
}

void PaxosAcceptor::OnPrepareRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (context->GetDatabase()->IsActive())
        return;

    senderID = imsg.nodeID;
    
    Log_Trace("state.promisedProposalID: %" PRIu64 " msg.proposalID: %" PRIu64 "",
     state.promisedProposalID, imsg.proposalID);
    
    if (imsg.proposalID < state.promisedProposalID)
    {
        omsg.PrepareRejected(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(),
         imsg.proposalID, state.promisedProposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.promisedProposalID = imsg.proposalID;

    if (!state.accepted)
        omsg.PrepareCurrentlyOpen(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
    else
        omsg.PreparePreviouslyAccepted(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(),
         imsg.proposalID, state.acceptedProposalID,
         state.acceptedRunID, state.acceptedValue);
    
    WriteState();
    //RLOG->StopPaxos(); TODO
}

void PaxosAcceptor::OnProposeRequest(PaxosMessage& imsg)
{
    Log_Trace();
    
    if (context->GetDatabase()->IsActive())
        return;
    
    senderID = imsg.nodeID;

    Log_Trace("state.promisedProposalID: %" PRIu64 " msg.proposalID: %" PRIu64 "",
     state.promisedProposalID, imsg.proposalID);
    
    if (imsg.proposalID < state.promisedProposalID)
    {
        omsg.ProposeRejected(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
        context->GetTransport()->SendMessage(senderID, omsg);
        return;
    }

    state.accepted = true;
    state.acceptedProposalID = imsg.proposalID;
    state.acceptedRunID = imsg.runID;
    state.acceptedValue.Write(imsg.value);
    omsg.ProposeAccepted(imsg.paxosID, REPLICATION_CONFIG->GetNodeID(), imsg.proposalID);
    
    WriteState();
    //RLOG->StopPaxos(); TODO
}

void PaxosAcceptor::OnStateWritten()
{
    Log_Trace();
    
    if (writtenPaxosID != context->GetPaxosID())
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
        db->GetAcceptedValue(state.acceptedValue);
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
        db->SetAcceptedValue(state.acceptedValue);
    }

    writtenPaxosID = context->GetPaxosID();

    db->Commit();

    // this is a different function because it may be necessary to async this
    // right now this makes writtenPaxosID (and senderID) unecessary
    OnStateWritten();
}
