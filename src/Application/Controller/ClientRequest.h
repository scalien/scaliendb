#ifndef CLIENTREQUEST_H
#define CLIENTREQUEST_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"

#define CLIENTREQUEST_GET_MASTER		'm'

#define CLIENTREQUEST_CREATE_DATABASE	'C'
#define CLIENTREQUEST_RENAME_DATABASE	'R'
#define CLIENTREQUEST_DELETE_DATABASE	'D'

#define CLIENTREQUEST_CREATE_TABLE		'c'
#define CLIENTREQUEST_RENAME_TABLE		'r'
#define CLIENTREQUEST_DELETE_TABLE		'd'

#define CLIENTREQUEST_GET				'G'
#define CLIENTREQUEST_SET				'S'
#define CLIENTREQUEST_DELETE			'X'

/*
===============================================================================

 ClientRequest

===============================================================================
*/

class ClientRequest
{
public:
	/* Variables */
	char		type;
	uint64_t	commandID;
	uint64_t	databaseID;
	uint64_t	tableID;
	uint64_t	shardID;
	ReadBuffer	name;
	ReadBuffer	key;
	ReadBuffer	value;

	bool		IsControllerRequest();
	bool		IsShardServerRequest();
	
	/* Master query */
	bool		GetMaster(uint64_t commandID);
	
	/* Database management */
	bool		CreateDatabase(
				 uint64_t commandID, ReadBuffer& name);
	bool		RenameDatabase(
				 uint64_t commandID, uint64_t databaseID, ReadBuffer& name);
	bool		DeleteDatabase(
				 uint64_t commandID, uint64_t databaseID);
	
	/* Table management */
	bool		CreateTable(
				 uint64_t commandID, uint64_t databaseID, ReadBuffer& name);
	bool		RenameTable(
				 uint64_t commandID, uint64_t databaseID, uint64_t tableID, ReadBuffer& name);
	bool		DeleteTable(
				 uint64_t commandID, uint64_t databaseID, uint64_t tableID);
	
	/* Data operations */
	bool		Set(
				 uint64_t commandID, uint64_t databaseID,
				 uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
	bool		Get(
				 uint64_t commandID, uint64_t databaseID,
				 uint64_t tableID, ReadBuffer& key);
	bool		Delete(
				 uint64_t commandID, uint64_t databaseID,
				 uint64_t tableID, ReadBuffer& key);
	
	/* Serialization */
	bool		Read(ReadBuffer buffer);
	bool		Write(Buffer& buffer);
};

#endif
