#ifndef CONFIGSTATE_H
#define CONFIGSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigQuorum.h"
#include "ConfigDatabase.h"
#include "ConfigTable.h"
#include "ConfigShard.h"
#include "ConfigShardServer.h"
#include "Application/Controller/ConfigCommand.h"

/*
===============================================================================

 ConfigState

===============================================================================
*/

class ConfigState
{
public:
	typedef InList<ConfigQuorum>		QuorumList;
	typedef InList<ConfigDatabase>		DatabaseList;
	typedef InList<ConfigTable>			TableList;
	typedef InList<ConfigShard>			ShardList;
	typedef InList<ConfigShardServer>	ShardServerList;

	// config data:
	
	QuorumList			quorums;
	DatabaseList		databases;
	TableList			tables;
	ShardList			shards;
	ShardServerList		shardServers;
	
	uint64_t			nextNodeID;
	
	void				Init();
	
	void				Read();
	void				Write();

	void				Update(ConfigCommand& command);
};

#endif
