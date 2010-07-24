#include "ReplicatedLog.h"
#include "ReplicationManager.h"
#include "System/Events/EventLoop.h"

static Buffer enableMultiPaxos;

void ReplicatedLog::Init(QuorumContext* context_)
{
	Log_Trace();
	
	context = context_;
	
//	replicatedDB = NULL; // TODO
	
	proposer.Init(context);
	acceptor.Init(context);
	learner.Init(context);
	
	proposer.paxosID = acceptor.paxosID;
	learner.paxosID = acceptor.paxosID;
	lastRequestChosenTime = 0;
	lastRequestChosenPaxosID = 0;
	enableMultiPaxos.Write("EnableMultiPaxos");
	
//	lastStarted = EventLoop::Now();
//	lastLength = 0;
//	lastTook = 0;
//	thruput = 0;
		
//	logCache.Init(acceptor.paxosID); // TODO
}

void ReplicatedLog::Append(const Buffer& value)
{
	Log_Trace();
	
//	if (!proposeQueue.Enqueue(value))
//		ASSERT_FAIL();
	
	if (!proposer.IsActive())
		proposer.Propose(value); //proposer.Propose(*logQueue.Next());
}

uint64_t ReplicatedLog::GetPaxosID() const
{
	return proposer.paxosID;
}

void ReplicatedLog::OnMessage(const PaxosMessage& imsg)
{
	Log_Trace();
	
	assert(learner.state.learned == false);
	
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
//	else if (imsg.type == PAXOS_START_CATCHUP)
//		OnStartCatchup();
	else
		ASSERT_FAIL();
}

void ReplicatedLog::OnPrepareRequest(const PaxosMessage& imsg)
{
	Log_Trace();
		
	if (imsg.paxosID == acceptor.paxosID)
		acceptor.OnPrepareRequest(imsg);

	OnRequest(imsg);
}

void ReplicatedLog::OnPrepareResponse(const PaxosMessage& imsg)
{
	Log_Trace();
	
	if (imsg.paxosID == proposer.paxosID)
		proposer.OnPrepareResponse(imsg);
}

void ReplicatedLog::OnProposeRequest(const PaxosMessage& imsg)
{
	Log_Trace();
	
	if (imsg.paxosID == acceptor.paxosID)
		acceptor.OnProposeRequest(imsg);
	
	OnRequest(imsg);
}

void ReplicatedLog::OnProposeResponse(const PaxosMessage& imsg)
{
	Log_Trace();

	if (imsg.paxosID == proposer.paxosID)
		proposer.OnProposeResponse(imsg);
}

void ReplicatedLog::OnLearnChosen(const PaxosMessage& imsg)
{
	Log_Trace();

	uint64_t		paxosID;
	bool			ownAppend, clientAppend, commit;

	if (imsg.paxosID > learner.paxosID)
	{
		//	I am lagging and need to catch-up
		learner.RequestChosen(imsg.nodeID);
		return;
	}

	if (imsg.paxosID < learner.paxosID)
		return;
	
	// copy from acceptor
	if (imsg.type == PAXOS_LEARN_PROPOSAL && acceptor.state.accepted
	 && acceptor.state.acceptedProposalID == imsg.proposalID)
	 {
		// HACK: change message type
		imsg.LearnValue(imsg.paxosID, RMAN->GetNodeID(),
		acceptor.state.acceptedRunID, acceptor.state.acceptedEpochID,
		&acceptor.state.acceptedValue);
	}
	
	if (imsg.type == PAXOS_LEARN_VALUE)
		learner.OnLearnChosen(imsg);
	else
	{
		learner.RequestChosen(imsg.nodeID);
		return;
	}
	
	// save it in the logCache (includes the epoch info)
	commit = learner.paxosID == (context->GetHighestPaxosID() - 1);
	logCache.Set(learner.paxosID, learner.state.value, commit);
	
	paxosID = learner.paxosID;		// this is the value we
									// pass to the ReplicatedDB
	
	NewPaxosRound();
	// increments paxosID, clears proposer, acceptor, learner
	
	if (context->GetHighestPaxosID() > paxosID)
		learner.RequestChosen(imsg.nodeID);
	
	if (imsg.nodeID == RMAN->GetNodeID()
	 && imsg.runID == RMAN->GetRunID()
	 && imsg.epochID == context->GetEpochID()
	 && context->IsLeader())
	{
//		logQueue.Pop(); // we just appended this
		proposer.state.multi = true;
		ownAppend = true;
		Log_Trace("Multi paxos enabled");
	}
	else
	{
		proposer.state.multi = false;
		ownAppend = false;
		Log_Trace("Multi paxos disabled");
	}
	
//	if (!GetTransaction()->IsActive())
//	{
//		Log_Trace("starting new transaction");
//		GetTransaction()->Begin();
//	}

	if (!Buffer::Cmp(*imsg.value, enableMultiPaxos))
	{
		clientAppend =
		 ownAppend && imsg.epochID == context->GetEpochID() && context->IsLeader();
//		replicatedDB->OnAppend(GetTransaction(), paxosID,  imsg.value, clientAppend);
	}

//	if (!proposer.IsActive() && logQueue.Length() > 0)
//		proposer.Propose(*logQueue.Next());
}

void ReplicatedLog::OnRequestChosen(const PaxosMessage& imsg)
{
	Log_Trace();
	
	Buffer* value;
	PaxosMessage omsg;
	
	if (imsg.paxosID >= learner.paxosID)
		return;
	
	// the node is lagging and needs to catch-up
	value = logCache.Get(imsg.paxosID);
	if (value != NULL)
	{
		Log_Trace("Sending paxosID %d to node %d", imsg.paxosID, imsg.nodeID);
		omsg.LearnValue(imsg.paxosID, RMAN->GetNodeID(), 0, 0, value);
		context->GetTransport()->SendMessage(imsg.nodeID, omsg);
	}
//	else // TODO
//	{
//		Log_Trace("Node requested a paxosID I no longer have");
//		learner.SendStartCatchup(pmsg.nodeID, pmsg.paxosID);
//	}
}

//void ReplicatedLog::OnStartCatchup()
//{
//	Log_Trace();
//
//	if (pmsg.paxosID == learner.paxosID &&
//		replicatedDB != NULL &&
//		!replicatedDB->IsCatchingUp() &&
//		masterLease.IsLeaseKnown())
//	{
//		replicatedDB->OnDoCatchup(masterLease.GetLeaseOwner());
//	}
//}

void ReplicatedLog::OnRequest(const PaxosMessage& imsg)
{
	Log_Trace();
	
	uint64_t			now;
	Buffer*			value;
	PaxosMessage	omsg;

	if (imsg.paxosID < acceptor.paxosID)
	{
		// the node is lagging and needs to catch-up
		value = logCache.Get(imsg.paxosID);
		if (value == NULL)
			return;
		omsg.LearnValue(imsg.paxosID, RMAN->GetNodeID(), 0, 0, value);
		context->GetTransport()->SendMessage(imsg.nodeID, omsg);
	}
	else // paxosID < msg.paxosID
	{
		//	I am lagging and need to catch-up
		learner.RequestChosen(imsg.nodeID);
	}
}

void ReplicatedLog::NewPaxosRound()
{
//	uint64_t now;
//	now = EventLoop::Now();
//	lastTook = ABS(now - lastStarted);
//	lastLength = learner.state.value.length;
//	thruput = (uint64_t)(lastLength / (lastTook / 1000.0));
//	lastStarted = now;
	
	EventLoop::Remove(&(proposer.prepareTimeout));
	EventLoop::Remove(&(proposer.proposeTimeout));
	proposer.paxosID++;
	proposer.state.Init();

	acceptor.paxosID++;
	acceptor.state.Init();

	learner.paxosID++;
	learner.state.Init();
}

void ReplicatedLog::OnLearnLease()
{
	// TODO: register or what?
	if (context->IsLeader() && !proposer.state.multi && !proposer.IsActive())
	{
		Log_Trace("Appending EnableMultiPaxos");
		Append(enableMultiPaxos);
	}
}

void ReplicatedLog::OnLeaseTimeout()
{
	// TODO: register or what?
	proposer.state.multi = false;
	
	if (replicatedDB)
	{
//		logQueue.Clear();
		proposer.Stop();
		replicatedDB->OnMasterLeaseExpired();
	}
}

bool ReplicatedLog::IsAppending()
{
	return context->IsLeader() && proposer.state.numProposals > 0;
}

//Transaction* ReplicatedLog::GetTransaction()
//{
//	return &acceptor.transaction;
//}

//bool ReplicatedLog::IsMultiRound()
//{
//	return proposer.state.multi;
//}

void ReplicatedLog::RegisterPaxosID(uint64_t paxosID, unsigned nodeID)
{
	if (paxosID > learner.paxosID)
	{
		//	I am lagging and need to catch-up
		learner.RequestChosen(nodeID);
	}
}

//uint64_t ReplicatedLog::GetLastRound_Length()
//{
//	return lastLength;
//}
//
//uint64_t ReplicatedLog::GetLastRound_Time()
//{
//	return lastTook;
//}
//
//uint64_t ReplicatedLog::GetLastRound_Thruput()
//{
//	return thruput;
//}
