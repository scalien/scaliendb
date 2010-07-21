#ifndef PAXOSLEARNER_H
#define PAXOSLEARNER_H

#include "System/Common.h"
#include "Framework/Replication/ReplicationContext.h"
#include "PaxosMessage.h"
#include "States/PaxosLearnerState.h"

#define REQUEST_CHOSEN_TIMEOUT	7000

/*
===============================================================================

 PaxosLearner

===============================================================================
*/

class PaxosLearner
{
public:
	void						Init(ReplicationContext* context);
	
	void						RequestChosen(uint64_t nodeID);
	void						SendChosen(uint64_t nodeID, uint64_t paxosID, const Buffer& value);	
//	bool						SendStartCatchup(uint64_t nodeID, uint64_t paxosID);	
	bool						IsLearned();
	const Buffer&				GetValue();

protected:
	void						OnLearnChosen(const PaxosMessage& msg);
	void						OnRequestChosen(const PaxosMessage& msg);

	ReplicationContext*			context;
	PaxosLearnerState			state;
	uint64_t					paxosID;
	uint64_t					lastRequestChosenTime;
	uint64_t					lastRequestChosenPaxosID;
};

#endif
