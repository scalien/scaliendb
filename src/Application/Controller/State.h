#ifndef CLUSTERSTATE_H
#define CLUSTERSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigDatabase.h"
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
	// for each table, we have a replication target
	// for each table, we have at least one shard
	//
	// for each shard, we have a last known paxosID
	// for each shard, we have a list of shard servers who store the
	//                 latest version (by paxosID, not versionID)
	// for each shard, we have a quorum consisting of shard servers
	// for each shard, we either have a primary or not

	typedef InList<ConfigDatabase>		DatabaseList;
	typedef InList<ClusterShardServer>	ShardServerList;

	// config data:
	DatabaseList		databases;
	// cluster data:
	ShardServerList		shardServers;
	
	void			Update(uint64_t nodeID, ClusterMessage& command);
	bool			GetMessage(ClusterMessage& msg);
};

#endif
