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

	// ========================================================================================
	//
	// Not replicated, only stored by the MASTER in-memory
	bool				hasMaster;
	uint64_t			masterID;
	// ========================================================================================
	
	QuorumList			quorums;
	DatabaseList		databases;
	TableList			tables;
	ShardList			shards;
	ShardServerList		shardServers;
	
	uint64_t			nextQuorumID;
	uint64_t			nextDatabaseID;
	uint64_t			nextTableID;
	uint64_t			nextShardID;
	uint64_t			nextNodeID;
	
	void				Init();

	ConfigQuorum*		GetQuorum(uint64_t quorumID);
	ConfigDatabase*		GetDatabase(uint64_t databaseID);
	ConfigDatabase*		GetDatabase(ReadBuffer& name);
	ConfigTable*		GetTable(uint64_t tableID);
	ConfigTable*		GetTable(uint64_t databaseID, ReadBuffer& name);
	ConfigShard*		GetShard(uint64_t shardID);
	ConfigShardServer*	GetShardServer(uint64_t nodeID);

	bool				CompleteMessage(ConfigMessage& message);
	bool				OnMessage(ConfigMessage& message);
	
	bool				Read(ReadBuffer& buffer, bool withVolatile = false);
	bool				Write(Buffer& buffer, bool withVolatile = false);

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

	bool				ReadQuorums(ReadBuffer& buffer, bool withVolatile);
	void				WriteQuorums(Buffer& buffer, bool withVolatile);

	bool				ReadDatabases(ReadBuffer& buffer);
	void				WriteDatabases(Buffer& buffer);

	bool				ReadTables(ReadBuffer& buffer);
	void				WriteTables(Buffer& buffer);

	bool				ReadShards(ReadBuffer& buffer);
	void				WriteShards(Buffer& buffer);

	bool				ReadShardServers(ReadBuffer& buffer);
	void				WriteShardServers(Buffer& buffer);
	
	bool				ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile);
	void				WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile);

	bool				ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer);
	void				WriteDatabase(ConfigDatabase& database, Buffer& buffer);

	bool				ReadTable(ConfigTable& database, ReadBuffer& buffer);
	void				WriteTable(ConfigTable& table, Buffer& buffer);

	bool				ReadShard(ConfigShard& database, ReadBuffer& buffer);
	void				WriteShard(ConfigShard& shard, Buffer& buffer);

	bool				ReadShardServer(ConfigShardServer& database, ReadBuffer& buffer);
	void				WriteShardServer(ConfigShardServer& shardServer, Buffer& buffer);

	bool				ReadIDList(List<uint64_t>& numbers, ReadBuffer& buffer);
	void				WriteIDList(List<uint64_t>& numbers, Buffer& buffer);
};

#endif
