#include "ConfigState.h"

#define HAS_LEADER_YES		'Y'
#define HAS_LEADER_NO		'N'

#define READ_SEPARATOR()	\
	read = buffer.Readf(":");		\
	if (read < 1) return false;	\
	buffer.Advance(read);

#define CHECK_ADVANCE(n)	\
	if (read < n)			\
		return false;		\
	buffer.Advance(read)

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

bool ConfigState::Read(ReadBuffer& buffer_, bool withVolatile)
{
	char		c;
	int			read;
	ReadBuffer	buffer;
	
	buffer = buffer_; // because of Advance()

	hasMaster = false;
	if (withVolatile)
	{
		READ_SEPARATOR();
		c = HAS_LEADER_NO;
		buffer.Readf("%c", &c);
		CHECK_ADVANCE(1);
		if (c == HAS_LEADER_YES)
		{
			READ_SEPARATOR();
			buffer.Readf("%U", &masterID);
		}
	}
	
	if (!ReadQuorums(buffer, withVolatile))
		return false;
	READ_SEPARATOR();
	if (!ReadDatabases(buffer))
		return false;
	READ_SEPARATOR();
	if (!ReadTables(buffer))
		return false;
	READ_SEPARATOR();
	if (!ReadShards(buffer))
		return false;
	READ_SEPARATOR();
	if (!ReadShardServers(buffer))
		return false;

	return (read == (signed)buffer_.GetLength());
}

bool ConfigState::Write(Buffer& buffer, bool withVolatile)
{
	if (withVolatile)
	{
		buffer.Appendf(":");
		if (hasMaster)
			buffer.Appendf("P:%U", masterID);
		else
			buffer.Appendf("N");
	}

	WriteQuorums(buffer, withVolatile);
	buffer.Appendf(":");
	WriteDatabases(buffer);
	buffer.Appendf(":");
	WriteTables(buffer);
	buffer.Appendf(":");
	WriteShards(buffer);
	buffer.Appendf(":");
	WriteShardServers(buffer);
	return true;
}

void ConfigState::OnMessage(ConfigMessage& message)
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
	
	List<uint64_t>& activeNodes = itQuorum->activeNodes;
	
	for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			return false; // node already in quorum
	}

	List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
	for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
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
	
	List<uint64_t>& activeNodes = itQuorum->activeNodes;
	for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			return true; // node in quorum
	}

	List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
	for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
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

void ConfigState::OnRegisterShardServer(ConfigMessage& message)
{
	ConfigShardServer*	it;
	
	// make sure nodeID is available
	assert(GetShardServer(message.nodeID) == NULL);
	
	it = new ConfigShardServer;
	it->nodeID = message.nodeID;
	it->endpoint = message.endpoint;
}

void ConfigState::OnCreateQuorum(ConfigMessage& message)
{
	ConfigQuorum*	it;
	
	// make sure quorumID is available
	assert(GetQuorum(message.quorumID) == NULL);
		
	it = new ConfigQuorum;
	it->quorumID = message.quorumID;
	it->activeNodes = message.nodes;
	it->productionType = message.productionType;
}

void ConfigState::OnIncreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	itQuorum = GetQuorum(message.quorumID);
	// make sure quorum exists
	assert(itQuorum != NULL);
		
	// make sure node is not already in quorum
	List<uint64_t>& activeNodes = itQuorum->activeNodes;
	for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			ASSERT_FAIL();
	}

	List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
	for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
			ASSERT_FAIL();
	}
	
	inactiveNodes.Append(message.nodeID);
}

void ConfigState::OnDecreaseQuorum(ConfigMessage& message)
{
	ConfigQuorum*	itQuorum;
	uint64_t*		itNodeID;
	
	itQuorum = GetQuorum(message.quorumID);
	// make sure quorum exists
	assert(itQuorum != NULL);
	
	// make sure node is in quorum
	List<uint64_t>& activeNodes = itQuorum->activeNodes;
	for (itNodeID = activeNodes.First(); itNodeID != NULL; itNodeID = activeNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
		{
			activeNodes.Remove(itNodeID);
			return;
		}
	}

	List<uint64_t>& inactiveNodes = itQuorum->inactiveNodes;
	for (itNodeID = inactiveNodes.First(); itNodeID != NULL; itNodeID = inactiveNodes.Next(itNodeID))
	{
		if (*itNodeID == message.nodeID)
		{
			inactiveNodes.Remove(itNodeID);
			return;
		}
	}
	
	ASSERT_FAIL();
}

void ConfigState::OnCreateDatabase(ConfigMessage& message)
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
}

void ConfigState::OnRenameDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;

	it = GetDatabase(message.name);
	// make sure database with name does not exist
	assert(it == NULL);
	it = GetDatabase(message.databaseID);
	// make sure database with ID exists
	assert(it != NULL);
	
	it->name.Write(message.name);
}

void ConfigState::OnDeleteDatabase(ConfigMessage& message)
{
	ConfigDatabase*	it;

	it = GetDatabase(message.databaseID);
	// make sure database with ID exists
	assert(it != NULL);

	databases.Delete(it);
}

void ConfigState::OnCreateTable(ConfigMessage& message)
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
}

void ConfigState::OnRenameTable(ConfigMessage& message)
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
}

void ConfigState::OnDeleteTable(ConfigMessage& message)
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
}

bool ConfigState::ReadQuorums(ReadBuffer& buffer, bool withVolatile)
{
	int				read;
	unsigned		num, i;
	ConfigQuorum*	quorum;
	
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		quorum = new ConfigQuorum;
		if (!ReadQuorum(*quorum, buffer, withVolatile))
			return false;
	}
	
	return true;
}

void ConfigState::WriteQuorums(Buffer& buffer, bool withVolatile)
{
	ConfigQuorum*	it;

	// write number of quorums
	buffer.Appendf("%u", quorums.GetLength());

	for (it = quorums.First(); it != NULL; it = quorums.Next(it))
	{
		buffer.Appendf(":");
		WriteQuorum(*it, buffer, withVolatile);
	}
}

bool ConfigState::ReadDatabases(ReadBuffer& buffer)
{
	int				read;
	unsigned		num, i;
	ConfigDatabase*	database;
	
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		database = new ConfigDatabase;
		if (!ReadDatabase(*database, buffer))
			return false;
	}
	
	return true;
}

void ConfigState::WriteDatabases(Buffer& buffer)
{
	ConfigDatabase*	it;
	
	// write number of databases
	buffer.Appendf("%u", databases.GetLength());
	
	for (it = databases.First(); it != NULL; it = databases.Next(it))
	{
		buffer.Appendf(":");
		WriteDatabase(*it, buffer);
	}
}

bool ConfigState::ReadTables(ReadBuffer& buffer)
{
	int				read;
	unsigned		num, i;
	ConfigTable*	table;
	
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		table = new ConfigTable;
		if (!ReadTable(*table, buffer))
			return false;
	}
	
	return true;
}

void ConfigState::WriteTables(Buffer& buffer)
{
	ConfigTable*	it;
	
	// write number of databases
	buffer.Appendf("%u", tables.GetLength());
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		buffer.Appendf(":");
		WriteTable(*it, buffer);
	}
}

bool ConfigState::ReadShards(ReadBuffer& buffer)
{
	int				read;
	unsigned		num, i;
	ConfigShard*	shard;
	
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		shard = new ConfigShard;
		if (!ReadShard(*shard, buffer))
			return false;
	}
	
	return true;
}

void ConfigState::WriteShards(Buffer& buffer)
{
	ConfigShard*	it;
	
	// write number of databases
	buffer.Appendf("%u", shards.GetLength());
	
	for (it = shards.First(); it != NULL; it = shards.Next(it))
	{
		buffer.Appendf(":");
		WriteShard(*it, buffer);
	}
}

bool ConfigState::ReadShardServers(ReadBuffer& buffer)
{
	int					read;
	unsigned			num, i;
	ConfigShardServer*	shardServer;
	
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		shardServer = new ConfigShardServer;
		if (!ReadShardServer(*shardServer, buffer))
			return false;
	}
	
	return true;
}

void ConfigState::WriteShardServers(Buffer& buffer)
{
	ConfigShardServer*	it;
	
	// write number of databases
	buffer.Appendf("%u", shardServers.GetLength());
	
	for (it = shardServers.First(); it != NULL; it = shardServers.Next(it))
	{
		buffer.Appendf(":");
		WriteShardServer(*it, buffer);
	}
}

bool ConfigState::ReadQuorum(ConfigQuorum& quorum, ReadBuffer& buffer, bool withVolatile)
{
	int		read;
	char	c;
	
	read = buffer.Readf("%U:%c", &quorum.quorumID, &quorum.productionType);
	CHECK_ADVANCE(3);
	READ_SEPARATOR();
	if (!ReadIDList(quorum.activeNodes, buffer))
		return false;
	READ_SEPARATOR();
	if (!ReadIDList(quorum.inactiveNodes, buffer))
		return false;
	READ_SEPARATOR();
	if (!ReadIDList(quorum.shards, buffer))
		return false;
	READ_SEPARATOR();
	
	quorum.hasPrimary = false;
	if (withVolatile)
	{
		READ_SEPARATOR();
		c = HAS_LEADER_NO;
		buffer.Readf("%c", &c);
		CHECK_ADVANCE(1);
		if (c == HAS_LEADER_YES)
		{
			READ_SEPARATOR();
			buffer.Readf("%U", &quorum.primaryID);
		}
	}
	
	return true;
}

void ConfigState::WriteQuorum(ConfigQuorum& quorum, Buffer& buffer, bool withVolatile)
{
	buffer.Appendf("%U:%c", quorum.quorumID, quorum.productionType);
	buffer.Appendf(":");
	WriteIDList(quorum.activeNodes, buffer);
	buffer.Appendf(":");
	WriteIDList(quorum.inactiveNodes, buffer);
	buffer.Appendf(":");
	WriteIDList(quorum.shards, buffer);

	if (withVolatile)
	{
		buffer.Appendf(":");
		if (quorum.hasPrimary)
			buffer.Appendf("P:%U", quorum.primaryID);
		else
			buffer.Appendf("N");
	}
}

bool ConfigState::ReadDatabase(ConfigDatabase& database, ReadBuffer& buffer)
{
	int read;
	
	read = buffer.Readf("%U:%#B:%c", &database.databaseID, &database.name, &database.productionType);
	CHECK_ADVANCE(5);
	READ_SEPARATOR();
	if (!ReadIDList(database.tables, buffer))
		return false;
		
	return true;
}

void ConfigState::WriteDatabase(ConfigDatabase& database, Buffer& buffer)
{
	buffer.Appendf("%U:%#B:%c", database.databaseID, &database.name, database.productionType);
	buffer.Appendf(":");
	WriteIDList(database.tables, buffer);
}

bool ConfigState::ReadTable(ConfigTable& table, ReadBuffer& buffer)
{
	int read;
	
	read = buffer.Readf("%U:%U:%#B", &table.databaseID, &table.tableID, &table.name);
	CHECK_ADVANCE(5);
	if (!ReadIDList(table.shards, buffer))
		return false;
	
	return true;
}

void ConfigState::WriteTable(ConfigTable& table, Buffer& buffer)
{
	buffer.Appendf("%U:%U:%#B", table.databaseID, table.tableID, &table.name);
	buffer.Appendf(":");
	WriteIDList(table.shards, buffer);
}

bool ConfigState::ReadShard(ConfigShard& shard, ReadBuffer& buffer)
{
	int read;
	
	read = buffer.Readf("%U:%U:%U:%U:%#B:%#B",
	 &shard.quorumID, &shard.databaseID, &shard.tableID,
	 &shard.shardID, &shard.firstKey, &shard.lastKey);
	CHECK_ADVANCE(11);
	return true;
}

void ConfigState::WriteShard(ConfigShard& shard, Buffer& buffer)
{
	buffer.Appendf("%U:%U:%U:%U:%#B:%#B",
	 shard.quorumID, shard.databaseID, shard.tableID,
	 shard.shardID, &shard.firstKey, &shard.lastKey);
}

bool ConfigState::ReadShardServer(ConfigShardServer& shardServer, ReadBuffer& buffer)
{
	int			read;
	ReadBuffer	rb;

	read = buffer.Readf("%U:%#R", &shardServer.nodeID, &rb);
	CHECK_ADVANCE(3);
	shardServer.endpoint.Set(rb);
	return true;
}

void ConfigState::WriteShardServer(ConfigShardServer& shardServer, Buffer& buffer)
{
	ReadBuffer endpoint;
	
	endpoint = shardServer.endpoint.ToReadBuffer();
	buffer.Appendf("%U:%#R", shardServer.nodeID, &endpoint);
}

bool ConfigState::ReadIDList(List<uint64_t>& IDs, ReadBuffer& buffer)
{
	uint64_t	ID;
	unsigned	i, num;
	int			read;
	
	num = 0;
	read = buffer.Readf("%u", &num);
	CHECK_ADVANCE(1);
	
	for (i = 0; i < num; i++)
	{
		READ_SEPARATOR();
		read = buffer.Readf("%U", &ID);
		CHECK_ADVANCE(1);
		IDs.Append(ID);
	}
	
	return true;
}

void ConfigState::WriteIDList(List<uint64_t>& IDs, Buffer& buffer)
{
	uint64_t*	it;
	
	// write number of items
	
	buffer.Appendf("%u", IDs.GetLength());
	
	for (it = IDs.First(); it != NULL; it = IDs.Next(it))
		buffer.Appendf(":%U", *it);
}

#undef READ_SEPARATOR
#undef CHECK_READ
