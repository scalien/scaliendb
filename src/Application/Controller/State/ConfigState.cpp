#include "ConfigState.h"

void ConfigState::Init()
{
	nextQuorumID = 1;
	nextDatabaseID = 1;
	nextTableID = 1;
	nextShardID = 1;
	nextNodeID = 1000;
}

bool ConfigState::CompleteMessage(ConfigMessage& message)
{
	switch (message.type)
	{
		/* Cluster management */
		case CONFIG_REGISTER_SHARDSERVER:
			return CompleteRegisterShardServer(message);
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

void ConfigState::Read()
{
}

void ConfigState::Write()
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

ConfigQuorum* ConfigState::GetQuorum(uint64_t quorumID)
{
	ConfigQuorum*	it;
	
	for (it = quorums.First(); it != NULL; it = quorums.Next(it))
	{
		if (it->quorumID == quorumID)
			return it;
	}
	
	return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(uint64_t databaseID)
{
	ConfigDatabase*	it;
	
	for (it =  databases.First(); it != NULL; it = databases.Next(it))
	{
		if (it->databaseID == databaseID)
			return it;
	}
	
	return NULL;
}

ConfigDatabase* ConfigState::GetDatabase(ReadBuffer& name)
{
	ConfigDatabase*	it;
	
	for (it = databases.First(); it != NULL; it = databases.Next(it))
	{
		if (BUFCMP(&it->name, &name))
			return it;
	}
	
	return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t tableID)
{
	ConfigTable*	it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->tableID == tableID)
			return it;
	}
	
	return NULL;
}

ConfigTable* ConfigState::GetTable(uint64_t databaseID, ReadBuffer& name)
{
	ConfigTable*	it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->databaseID == databaseID && BUFCMP(&it->name, &name))
			return it;
	}
	
	return NULL;
}

ConfigShard* ConfigState::GetShard(uint64_t shardID)
{
	ConfigShard*	it;
	
	for (it = shards.First(); it != NULL; it = shards.Next(it))
	{
		if (it->shardID == shardID)
			return it;
	}
	
	return NULL;
}

ConfigShardServer* ConfigState::GetShardServer(uint64_t nodeID)
{
	ConfigShardServer*	it;
	
	for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
	{
		if (it->nodeID == nodeID)
			return it;
	}
	
	return NULL;
}

bool ConfigState::CompleteRegisterShardServer(ConfigMessage& message)
{
	message.nodeID = nextNodeID++;
	return true;
}

bool ConfigState::CompleteCreateQuorum(ConfigMessage& message)
{
	uint64_t*	it;
	
	for (it = message.nodes.First(); it != NULL; it = message.nodes.Next(it))
	{
		if (GetShardServer(*it) == NULL)
			return false; // no such shard server
	}

	message.quorumID = nextQuorumID++;
	
	return true;
}

bool ConfigState::CompleteIncreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	itQuorum = GetQuorum(message.quorumID);
	if (itQuorum == NULL)
		return false; // no such quorum
	
	List<uint64_t>& nodes = itQuorum->nodes;
	
	for (itNodeID = nodes.First(); itNodeID != NULL; itNodeID = nodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			return false; // node already in quorum
	}
	
	return true;
}

bool ConfigState::CompleteDecreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	itQuorum = GetQuorum(message.quorumID);
	if (itQuorum == NULL)
		return false; // no such quorum
	
	List<uint64_t>& nodes = itQuorum->nodes;
	
	for (itNodeID = nodes.First(); itNodeID != NULL; itNodeID = nodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			return true; // node in quorum
	}
	
	return false; // node not in quorum
}

bool ConfigState::CompleteCreateDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;
	
	it = GetDatabase(message.name);
	if (it != NULL)
		return false; // database with name exists

	message.databaseID = nextDatabaseID++;
	return true;
}

bool ConfigState::CompleteRenameDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;
	
	it = GetDatabase(message.databaseID);
	if (it == NULL)
		return false; // no such database

	it = GetDatabase(message.name);
	if (it != NULL)
		return false; // database with name exists

	return true;
}

bool ConfigState::CompleteDeleteDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;
	
	it = GetDatabase(message.databaseID);
	if (it == NULL)
		return false; // no such database

	return true;
}

bool ConfigState::CompleteCreateTable(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;
	
	itQuorum = GetQuorum(message.quorumID);	
	if (itQuorum == NULL)
		return false; // no such quorum

	itDatabase = GetDatabase(message.databaseID);
	if (itDatabase == NULL)
		return false; // no such database

	itTable = GetTable(message.databaseID, message.name);
	if (itTable != NULL)
		return false; // table with name exists in database

	message.tableID = nextTableID++;
	message.shardID = nextShardID++;
	return true;
}

bool ConfigState::CompleteRenameTable(ConfigMessage& message)
{
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;

	itDatabase = GetDatabase(message.databaseID);
	if (itDatabase == NULL)
		return false; // no such database

	itTable = GetTable(message.tableID);
	if (itTable == NULL)
		return false; // no such table
	itTable = GetTable(message.databaseID, message.name);
	if (itTable != NULL);
		return false; // table with name exists in database

	return true;
}

bool ConfigState::CompleteDeleteTable(ConfigMessage& message)
{
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;

	itDatabase = GetDatabase(message.databaseID);
	if (itDatabase == NULL)
		return false; // no such database

	itTable = GetTable(message.tableID);
	if (itTable == NULL)
		return false; // no such table

	return true;
}

bool ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
	ConfigShardServer*	it;
	
	// make sure nodeID is available
	assert(GetShardServer(message.nodeID) == NULL);
	
	it = new ConfigShardServer;
	it->nodeID = message.nodeID;
	it->endpoint = message.endpoint;
	return true;
}

bool ConfigState::OnCreateQuorum(ConfigMessage& message)
{
	ConfigQuorum*	it;
	
	// make sure quorumID is available
	assert(GetQuorum(message.quorumID) == NULL);
		
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
	
	itQuorum = GetQuorum(message.quorumID);
	// make sure quorum exists
	assert(itQuorum != NULL);
		
	List<uint64_t>& nodes = itQuorum->nodes;
	
	// make sure node is not already in quorum
	for (itNodeID = nodes.First(); itNodeID != NULL; itNodeID = nodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			ASSERT_FAIL();
	}
	
	nodes.Append(message.nodeID);
	return true;
}

bool ConfigState::OnDecreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	itQuorum = GetQuorum(message.quorumID);
	// make sure quorum exists
	assert(itQuorum != NULL);
	
	List<uint64_t>& nodes = itQuorum->nodes;
	
	// make sure node is in quorum
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
	
	it = GetDatabase(message.databaseID);
	// make sure database with ID does not exist
	assert(it == NULL);
	it = GetDatabase(message.name);
	// make sure database with name does not exist
	assert(it == NULL);
		
	it = new ConfigDatabase;
	it->databaseID = message.databaseID;
	it->name.Write(message.name);
	databases.Append(it);
	return true;
}

bool ConfigState::OnRenameDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;

	it = GetDatabase(message.name);
	// make sure database with name does not exist
	assert(it == NULL);
	it = GetDatabase(message.databaseID);
	// make sure database with ID exists
	assert(it != NULL);
	
	it->name.Write(message.name);
	return true;
}

bool ConfigState::OnDeleteDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;

	it = GetDatabase(message.databaseID);
	// make sure database with ID exists
	assert(it != NULL);

	databases.Delete(it);
	return true;
}

bool ConfigState::OnCreateTable(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;
	ConfigShard*	itShard;

	itQuorum = GetQuorum(message.quorumID);
	// make sure quorum exists
	assert(itQuorum != NULL);

	itDatabase = GetDatabase(message.databaseID);
	// make sure database exists
	assert(itDatabase != NULL);

	itTable = GetTable(message.tableID);
	// make sure table with ID does not exist
	assert(itTable == NULL);
	itTable = GetTable(message.databaseID, message.name);
	// make sure table with name in database does not exist
	assert(itTable == NULL);

	itShard = GetShard(message.shardID);
	// make sure shard does not exist
	assert(itShard == NULL);
	
	itShard = new ConfigShard;
	itShard->quorumID = message.quorumID;
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
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;
	
	itDatabase = GetDatabase(message.databaseID);
	// make sure database exists
	assert(itDatabase != NULL);
	itTable = GetTable(message.tableID);
	// make sure table with ID exists
	assert(itTable != NULL);
	itTable = GetTable(message.databaseID, message.name);
	// make sure table with name does not exist
	assert(itTable == NULL);
	
	itTable->name.Write(message.name);
	return true;
}

bool ConfigState::OnDeleteTable(ConfigMessage& message)
{
	ConfigDatabase*	itDatabase;
	ConfigTable*	itTable;
	
	itDatabase = GetDatabase(message.databaseID);
	// make sure database exists
	assert(itDatabase != NULL);
	itTable = GetTable(message.tableID);
	// make sure table exists
	assert(itTable != NULL);
	
	tables.Delete(itTable);
	return true;
}
