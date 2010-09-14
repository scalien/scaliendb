#ifndef CONFIGSTATE_H
#define CONFIGSTATE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "ConfigQuorum.h"
#include "ConfigDatabase.h"
#include "ConfigTable.h"
#include "ConfigShard.h"
#include "ConfigShardServer.h"
#include "Application/Controller/ConfigMessage.h"

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
	
	uint64_t			nextQuorumID;
	uint64_t			nextDatabaseID;
	uint64_t			tableID;
	uint64_t			nextShardID;
	uint64_t			nextNodeID;
	
	void				Init();

	bool				CompleteMessage(ConfigMessage& message);
	bool				OnMessage(ConfigMessage& message);
	
	void				Read();
	void				Write();

private:
	bool				CompleteRegisterShardServer(ConfigMessage& message);
	bool				CompleteCreateQuorum(ConfigMessage& message);
	bool				CompleteIncreaseQuorum(ConfigMessage& message);
	bool				CompleteDecreaseQuorum(ConfigMessage& message);
	bool				CompleteCreateDatabase(ConfigMessage& message);
	bool				CompleteRenameDatabase(ConfigMessage& message);
	bool				CompleteDeleteDatabase(ConfigMessage& message);
	bool				CompleteCreateTable(ConfigMessage& message);
	bool				CompleteRenameTable(ConfigMessage& message);
	bool				CompleteDeleteTable(ConfigMessage& message);

	bool				OnRegisterShardServer(ConfigMessage& message);
	bool				OnCreateQuorum(ConfigMessage& message);
	bool				OnIncreaseQuorum(ConfigMessage& message);
	bool				OnDecreaseQuorum(ConfigMessage& message);
	bool				OnCreateDatabase(ConfigMessage& message);
	bool				OnRenameDatabase(ConfigMessage& message);
	bool				OnDeleteDatabase(ConfigMessage& message);
	bool				OnCreateTable(ConfigMessage& message);
	bool				OnRenameTable(ConfigMessage& message);
	bool				OnDeleteTable(ConfigMessage& message);
};

#endif
