#ifndef PAXOSLEARNER_H
#define PAXOSLEARNER_H

#include "System/Common.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosMessage.h"
#include "States/PaxosLearnerState.h"

#define REQUEST_CHOSEN_TIMEOUT	7000

class ReplicatedLog; // forward

/*
===============================================================================

 PaxosLearner

===============================================================================
*/

class PaxosLearner
{
public:
	void						Init(QuorumContext* context);
	
	void						RequestChosen(uint64_t nodeID);
//	bool						SendStartCatchup(uint64_t nodeID, uint64_t paxosID);	
	bool						IsLearned();
	const Buffer&				GetValue();

protected:
	void						OnLearnChosen(const PaxosMessage& msg);

	QuorumContext*				context;
	PaxosLearnerState			state;
	uint64_t					paxosID;
	uint64_t					lastRequestChosenTime;
	uint64_t					lastRequestChosenPaxosID;

	friend class ReplicatedLog;
};

#endif
