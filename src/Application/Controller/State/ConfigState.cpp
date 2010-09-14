#include "ConfigState.h"

void ConfigState::Init()
{
	nextQuorumID = 1;
	nextDatabaseID = 1;
	tableID = 1;
	nextShardID = 1;
	nextNodeID = 1000;
}

bool ConfigState::CompleteMessage(ConfigMessage& message)
{
	switch (message.type)
	{
		/* Cluster management */
		case CONFIG_REGISTER_SHARDSERVER:
			return CompleteShardServer(message);
		case CONFIG_CREATE_QUORUM:
			return CompleteCreateQuorum(message);
		case CONFIG_INCREASE_QUORUM:
			return CompleteIncreaseQuorum(message);
		case CONFIG_DECREASE_QUORUM:
			return CompleteDecreaseQuorum(message);

		/* Database management */
		case CONFIG_CREATE_DATABASE:
			return CompleteCreateDatabase(message);
		case CONFIG_RENAME_DATABASE:
			return CompleteRenameDatabase(message);
		case CONFIG_DELETE_DATABASE:
			return CompleteDeleteDatabase(message);

		/* Table management */
		case CONFIG_CREATE_TABLE:
			return CompleteCreateTable(message);
		case CONFIG_RENAME_TABLE:
			return CompleteRenameTable(message);
		case CONFIG_DELETE_TABLE:
			return CompleteDeleteTable(message);
		
		default:
			ASSERT_FAIL(); 
	}
}

bool ConfigState::CompleteRegisterShardServer(ConfigMessage& message)
{
}

bool ConfigState::CompleteCreateQuorum(ConfigMessage& message)
{
}

bool ConfigState::CompleteIncreaseQuorum(ConfigMessage& message)
{
}

bool ConfigState::CompleteDecreaseQuorum(ConfigMessage& message)
{
}

bool ConfigState::CompleteCreateDatabase(ConfigMessage& message)
{
}

bool ConfigState::CompleteRenameDatabase(ConfigMessage& message)
{
}

bool ConfigState::CompleteDeleteDatabase(ConfigMessage& message)
{
}

bool ConfigState::CompleteCreateTable(ConfigMessage& message)
{
}

bool ConfigState::CompleteRenameTable(ConfigMessage& message)
{
}

bool ConfigState::CompleteDeleteTable(ConfigMessage& message)
{
}

bool ConfigState::OnMessage(ConfigMessage& message)
{
	switch (message.type)
	{
		/* Cluster management */
		case CONFIG_REGISTER_SHARDSERVER:
			return OnRegisterShardServer(message);
		case CONFIG_CREATE_QUORUM:
			return OnCreateQuorum(message);
		case CONFIG_INCREASE_QUORUM:
			return OnIncreaseQuorum(message);
		case CONFIG_DECREASE_QUORUM:
			return OnDecreaseQuorum(message);

		/* Database management */
		case CONFIG_CREATE_DATABASE:
			return OnCreateDatabase(message);
		case CONFIG_RENAME_DATABASE:
			return OnRenameDatabase(message);
		case CONFIG_DELETE_DATABASE:
			return OnDeleteDatabase(message);

		/* Table management */
		case CONFIG_CREATE_TABLE:
			return OnCreateTable(message);
		case CONFIG_RENAME_TABLE:
			return OnRenameTable(message);
		case CONFIG_DELETE_TABLE:
			return OnDeleteTable(message);
		
		default:
			ASSERT_FAIL(); 
	}
}

void ConfigState::Read()
{
}

void ConfigState::Write()
{
}

bool ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
	ConfigShardServer*	it;
	
	for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
	{
		if (message.nodeID == it->nodeID)
			ASSERT_FAIL();
	}
	
	it = new ConfigShardServer;
	it->nodeID = message.nodeID;
	it->endpoint = message.endpoint;
	return true;
}

bool ConfigState::OnCreateQuorum(ConfigMessage& message)
{
	ConfigQuorum*	it;
	
	for (it = quorums.First(); it != NULL; it = quorums.Next(it))
	{
		if (message.quorumID = it->quorumID)
			ASSERT_FAIL();
	}
	
	it = new ConfigQuorum;
	it->quorumID = message.quorumID;
	it->nodes = message.nodes;
	it->productionType = message.productionType;
	return true;
}

bool ConfigState::OnIncreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = quorums.Next(itQuorum))
	{
		if (message.quorumID = itQuorum->quorumID)
			break;
	}
	
	assert(itQuorum != NULL);
	
	List<uint64_t>& nodes = itQuorum->nodes;
	
	for (itNodeID = nodes.First(); itNodeID != NULL; itNodeID = nodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
		{
			ASSERT_FAIL();
		}
	}
	
	nodes.Append(message.nodeID);
	return true;
}

bool ConfigState::OnDecreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = quorums.Next(itQuorum))
	{
		if (message.quorumID = itQuorum->quorumID)
			break;
	}
	
	assert(itQuorum != NULL);
	
	List<uint64_t>& nodes = itQuorum->nodes;
	
	for (itNodeID = nodes.First(); itNodeID != NULL; itNodeID = nodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
		{
			nodes.Remove(itNodeID);
			return true;
		}
	}
	
	ASSERT_FAIL();
}

bool ConfigState::OnCreateDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;
	
	for (it = databases.First(); it != NULL; it = databases.Next(it))
	{
		if (it->databaseID == message.databaseID)
			ASSERT_FAIL();
		if (BUFCMP(&it->name, &message.name))
			return false;
	}
	
	it = new ConfigDatabase;
	it->databaseID = message.databaseID;
	it->name.Write(message.name);
	databases.Append(it);
	return true;
}

bool ConfigState::OnRenameDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it, *db;
	
	for (it = databases.First(); it != NULL; it = databases.Next(it))
	{
		if (it->databaseID == message.databaseID)
			db = it;
		else if (BUFCMP(&it->name, &message.name))
			return false;
	}
	
	assert(db != NULL);
	
	db->name.Write(message.name);
	return true;
}

bool ConfigState::OnDeleteDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;
	
	for (it = databases.First(); it != NULL; it = databases.Next(it))
	{
		if (it->databaseID == message.databaseID)
		{
			databases.Remove(it);
			return true;
		}
	}
	
	ASSERT_FAIL();
}

bool ConfigState::OnCreateTable(ConfigMessage& message)
{
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;
	ConfigShard*	itShard;
	
	for (itDatabase = databases.First(); itDatabase != NULL; itDatabase = databases.Next(itDatabase))
	{
		if (itDatabase->databaseID == message.databaseID)
			break;
	}

	assert(itDatabase != NULL);
	
	for (itTable = tables.First(); itTable != NULL; itTable = tables.Next(itTable))
	{
		if (itTable->tableID == message.tableID)
			ASSERT_FAIL();
		if (itTable->databaseID == message.databaseID && BUFCMP(&itTable->name, &message.name))
			return false;
	}
	
	for (itShard = shards.First(); itShard != NULL; itShard = shards.Next(itShard))
	{
		if (itShard->shardID == message.shardID)
			ASSERT_FAIL();
	}
	
	itShard = new ConfigShard;
	itShard->databaseID = message.databaseID;
	itShard->tableID = message.tableID;
	itShard->shardID = message.shardID;
	shards.Append(itShard);
	
	itTable = new ConfigTable;
	itTable->databaseID = message.databaseID;
	itTable->tableID = message.tableID;
	itTable->shards.Append(message.shardID);
	tables.Append(itTable);
	return true;
}

bool ConfigState::OnRenameTable(ConfigMessage& message)
{
	ConfigTable*	it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->tableID == message.tableID)
			break;
	}

	assert(it != NULL);
	
	it->name.Write(message.name);
	return true;
}

bool ConfigState::OnDeleteTable(ConfigMessage& message)
{
	ConfigTable*	it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->tableID == message.tableID)
			break;
	}

	assert(it != NULL);
	
	tables.Delete(it);
	return true;
}
