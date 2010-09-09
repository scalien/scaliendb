#ifndef REPLICATIONCONFIG_H
#define REPLICATIONCONFIG_H

#include "System/Common.h"

#define	REPLICATED_CONFIG (ReplicationConfig::Get())

/*
===============================================================================

 ReplicationConfig

===============================================================================
*/

class ReplicationConfig
{
public:
	static ReplicationConfig* Get();
	
	void					SetNodeID(uint64_t nodeID);
	uint64_t				GetNodeID();

	void					SetRunID(uint64_t runID);
	uint64_t				GetRunID();
	
	uint64_t				NextProposalID(uint64_t proposalID);
	
private:
	ReplicationConfig();

	uint64_t				nodeID;
	uint64_t				runID;
};

#endif
