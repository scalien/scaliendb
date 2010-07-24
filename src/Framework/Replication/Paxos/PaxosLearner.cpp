#include "PaxosLearner.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosLearner::Init(QuorumContext* context_)
{
	context = context_;

	state.Init();
	lastRequestChosenTime = 0;
	lastRequestChosenPaxosID = 0;
}

void PaxosLearner::RequestChosen(uint64_t nodeID)
{
	PaxosMessage omsg;
	
	Log_Trace();
	
	if (lastRequestChosenPaxosID == paxosID &&
	 EventLoop::Now() - lastRequestChosenTime < REQUEST_CHOSEN_TIMEOUT)
		return;
	
	lastRequestChosenPaxosID = paxosID;
	lastRequestChosenTime = EventLoop::Now();
	
	omsg.RequestChosen(paxosID, RMAN->GetNodeID());
	
	context->GetTransport()->SendMessage(nodeID, omsg);
}

//bool PaxosLearner::SendStartCatchup(uint64_t nodeID,
//									uint64_t paxosID)
//{
//	Log_Trace();
//	
//	msg.StartCatchup(paxosID, RCONF->GetNodeID());
//	
//	msg.Write(wdata);
//
//	writers[nodeID]->Write(wdata);
//	
//	return true;
//}

void PaxosLearner::OnLearnChosen(const PaxosMessage& imsg)
{
	Log_Trace();

	state.learned = true;
	state.value.Write(*imsg.value);

	Log_Trace("+++ Consensus for paxosID = %" PRIu64 " is %.*s +++",
	 paxosID, P(&state.value));
}

bool PaxosLearner::IsLearned()
{
	return state.learned;	
}

const Buffer& PaxosLearner::GetValue()
{
	return state.value;
}
