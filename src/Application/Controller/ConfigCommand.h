#ifndef CONFIGCOMMAND_H
#define CONFIGCOMMAND_H

#include "Command.h"
#include "System/Platform.h"
#include "System/IO/Endpoint.h"
#include "System/Buffers/ReadBuffer.h"

#define CONFIG_REGISTER_SHARDSERVER		'S'
#define CONFIG_SPLIT_SHARD				'N'
#define CONFIG_INCREASE_SHARDQUORUM		'P'
#define CONFIG_DECREASE_SHARDQUORUM		'M'

#define CONFIG_CREATE_DATABASE			'C'
#define CONFIG_RENAME_DATABASE			'R'
#define CONFIG_DELETE_DATABASE			'D'

#define CONFIG_CREATE_TABLE				'c'
#define CONFIG_RENAME_TABLE				'r'
#define CONFIG_DELETE_TABLE				'd'

/*
===============================================================================

 ConfigCommand

===============================================================================
*/

class ConfigCommand : public Command
{
public:
	/* Variables */
	char		type;
	uint64_t	nodeID;
	Endpoint	endpoint;
	uint64_t	databaseID;
	uint64_t	tableID;
	ReadBuffer	name;
	uint64_t	shardID;
	uint64_t	newShardID;
	ReadBuffer	key;

	/* Cluster management */
	bool		RegisterShardServer(
				 uint64_t nodeID, Endpoint& endpoint);
	bool		SplitShard(
				 uint64_t shardID, uint64_t newShardID, ReadBuffer& key);
	bool		IncreaseShardQuorum(
				 uint64_t shardID, uint64_t nodeID);
	bool		DecreaseShardQuorum(
				 uint64_t shardID, uint64_t nodeID);

	/* Database management*/
	bool		CreateDatabase(
				 uint64_t databaseID, ReadBuffer& name);
	bool		RenameDatabase(
				 uint64_t databaseID, ReadBuffer& name);
	bool		DeleteDatabase(
				 uint64_t databaseID);

	/* Table management*/
	bool		CreateTable(
				 uint64_t databaseID, uint64_t tableID, uint64_t shardID, ReadBuffer& name);
	bool		RenameTable(
				 uint64_t databaseID, uint64_t tableID, ReadBuffer& name);
	bool		DeleteTable(
				 uint64_t databaseID, uint64_t tableID);
	
	/* Serialization */
	bool		Read(ReadBuffer buffer);
	bool		Write(Buffer& buffer);
};

#endif
