#ifndef CLUSTERSTATE_H
#define CLUSTERSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigQuorum.h"
#include "ConfigDatabase.h"
#include "ConfigTable.h"
#include "ConfigShard.h"
#include "ClusterShardServer.h"
#include "Application/Controller/ConfigCommand.h"
#include "Application/Common/ClusterMessage.h"

/*
===============================================================================

 State

===============================================================================
*/

class State
{
public:
	typedef InList<ConfigQuorum>		QuorumList;
	typedef InList<ConfigDatabase>		DatabaseList;
	typedef InList<ConfigTable>			TableList;
	typedef InList<ConfigShard>			ShardList;

	// config data:
	QuorumList			quorums;
	DatabaseList		databases;
	TableList			tables;
	ShardList			shards;
	
	uint64_t			nextNodeID;
	
	void				Init() { nextNodeID = 100; }
	
	void				Update(uint64_t nodeID, ClusterMessage& command);
	bool				GetMessage(ClusterMessage& msg);
};

#endif
