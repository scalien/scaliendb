#include "ConfigMessage.h"
#include "System/Buffers/Buffer.h"

bool ConfigMessage::RegisterShardServer(
 uint64_t nodeID_, Endpoint& endpoint_)
{
	type = CONFIG_REGISTER_SHARDSERVER;
	nodeID = nodeID_;
	endpoint = endpoint_;

	return true;
}

bool ConfigMessage::SplitShard(
 uint64_t shardID_, uint64_t newShardID_, ReadBuffer& key_)
{
	type = CONFIG_SPLIT_SHARD;
	shardID = shardID_;
	newShardID = newShardID_;
	key = key_;
	return true;
}

bool ConfigMessage::IncreaseShardQuorum(
 uint64_t shardID_, uint64_t nodeID_)
{
	type = CONFIG_INCREASE_SHARDQUORUM;
	shardID = shardID_;
	nodeID = nodeID_;
	return true;
}

bool ConfigMessage::DecreaseShardQuorum(
 uint64_t shardID_, uint64_t nodeID_)
{
	type = CONFIG_DECREASE_SHARDQUORUM;
	shardID = shardID_;
	nodeID = nodeID_;
	return true;
}

bool ConfigMessage::CreateDatabase(
 uint64_t databaseID_, ReadBuffer& name_)
{
	type = CONFIG_CREATE_DATABASE;
	databaseID = databaseID_;
	name = name_;
	return true;
}

bool ConfigMessage::RenameDatabase(
 uint64_t databaseID_, ReadBuffer& name_)
{
	type = CONFIG_RENAME_DATABASE;
	databaseID = databaseID_;
	name = name_;
	return true;
}

bool ConfigMessage::DeleteDatabase(
 uint64_t databaseID_)
{
	type = CONFIG_DELETE_DATABASE;
	databaseID = databaseID_;
	return true;
}

bool ConfigMessage::CreateTable(
 uint64_t databaseID_, uint64_t tableID_, uint64_t shardID_, ReadBuffer& name_)
{
	type = CONFIG_CREATE_TABLE;
	databaseID = databaseID_;
	tableID = tableID_;
	shardID = shardID_;
	name = name_;
	return true;
}

bool ConfigMessage::RenameTable(
 uint64_t databaseID_, uint64_t tableID_, ReadBuffer& name_)
{
	type = CONFIG_RENAME_TABLE;
	databaseID = databaseID_;
	tableID = tableID_;
	name = name_;
	return true;
}

bool ConfigMessage::DeleteTable(
 uint64_t databaseID_, uint64_t tableID_)
{
	type = CONFIG_DELETE_TABLE;
	databaseID = databaseID_;
	tableID = tableID_;
	return true;
}

bool ConfigMessage::Read(ReadBuffer buffer)
{
	int			read;
	ReadBuffer	rb;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		/* Cluster management */
		case CONFIG_REGISTER_SHARDSERVER:
			read = buffer.Readf("%c:%U:%#R",
			 &type, &nodeID, &rb);
			endpoint.Set(rb);
			break;
		case CONFIG_SPLIT_SHARD:
			read = buffer.Readf("%c:%U:%U:%#R",
			 &type, &shardID, &newShardID, &key);
			break;
		case CONFIG_INCREASE_SHARDQUORUM:
			read = buffer.Readf("%c:%U:%U",
			 &type, &shardID, &nodeID);
			break;
		case CONFIG_DECREASE_SHARDQUORUM:
			read = buffer.Readf("%c:%U:%U",
			 &type, &shardID, &nodeID);
			break;

		/* Database management */
		case CONFIG_CREATE_DATABASE:
			read = buffer.Readf("%c:%U:%#R",
			 &type, &databaseID, &name);
			break;
		case CONFIG_RENAME_DATABASE:
			read = buffer.Readf("%c:%U:%#R",
			 &type, &databaseID, &name);
			break;
		case CONFIG_DELETE_DATABASE:
			read = buffer.Readf("%c:%U", 
			&type, &databaseID);
			break;

		/* Table management */
		case CONFIG_CREATE_TABLE:
			read = buffer.Readf("%c:%U:%U:%U:%#R",
			 &type, &databaseID, &tableID, &shardID, &name);
			break;
		case CONFIG_RENAME_TABLE:
			read = buffer.Readf("%c:%U:%U:%#R",
			 &type, &databaseID, &tableID, &name);
			break;
		case CONFIG_DELETE_TABLE:
			read = buffer.Readf("%c:%U:%U",
			 &type, &databaseID, &tableID);
			break;
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);
}

bool ConfigMessage::Write(Buffer& buffer)
{
	ReadBuffer		rb;
	
	switch (type)
	{
		/* Cluster management */
		case CONFIG_REGISTER_SHARDSERVER:
			rb = endpoint.ToReadBuffer();
			buffer.Writef("%c:%U:%#R",
			 type, nodeID, &rb);
			return true;
		case CONFIG_SPLIT_SHARD:
			buffer.Writef("%c:%U:%U:%#R",
			 type, shardID, newShardID, &key);
			break;
		case CONFIG_INCREASE_SHARDQUORUM:
			buffer.Writef("%c:%U:%U",
			 type, shardID, nodeID);
			break;
		case CONFIG_DECREASE_SHARDQUORUM:
			buffer.Writef("%c:%U:%U",
			 type, shardID, nodeID);
			break;

		/* Database management */
		case CONFIG_CREATE_DATABASE:
			buffer.Writef("%c:%U:%#R",
			 type, databaseID, &name);
			break;
		case CONFIG_RENAME_DATABASE:
			buffer.Writef("%c:%U:%#R",
			 type, databaseID, &name);
			break;
		case CONFIG_DELETE_DATABASE:
			buffer.Writef("%c:%U",
			 type, databaseID);
			break;

		/* Table management */
		case CONFIG_CREATE_TABLE:
			buffer.Writef("%c:%U:%U:%U:%#R",
			 type, databaseID, tableID, shardID, &name);
			break;
		case CONFIG_RENAME_TABLE:
			buffer.Writef("%c:%U:%U:%#R",
			 type, databaseID, tableID, &name);
			break;
		case CONFIG_DELETE_TABLE:
			buffer.Writef("%c:%U:%U",
			 type, databaseID, tableID);
			break;
		default:
			return false;
	}
	
	return true;
}
