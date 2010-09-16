#ifndef CLIENTREQUEST_H
#define CLIENTREQUEST_H

#include "System/Containers/List.h"
#include "Framework/Messaging/Message.h"

#define CLIENTREQUEST_GET_MASTER		'm'

#define CLIENTREQUEST_GET_CONFIG_STATE	'A'

#define CLIENTREQUEST_CREATE_QUORUM		'Q'

#define CLIENTREQUEST_CREATE_DATABASE	'C'
#define CLIENTREQUEST_RENAME_DATABASE	'R'
#define CLIENTREQUEST_DELETE_DATABASE	'D'

#define CLIENTREQUEST_CREATE_TABLE		'c'
#define CLIENTREQUEST_RENAME_TABLE		'r'
#define CLIENTREQUEST_DELETE_TABLE		'd'

#define CLIENTREQUEST_GET				'G'
#define CLIENTREQUEST_SET				'S'
#define CLIENTREQUEST_DELETE			'X'

#define CLIENTREQUEST_PRODUCTION		'P'
#define CLIENTREQUEST_TEST				'T'


/*
===============================================================================================

 ClientRequest

===============================================================================================
*/

class ClientRequest : public Message
{
public:
	typedef List<uint64_t> NodeList;

	/* Variables */
	char		type;
	char		productionType;
	uint64_t	commandID;
	uint64_t	quorumID;
	uint64_t	databaseID;
	uint64_t	tableID;
	uint64_t	shardID;
	ReadBuffer	name;
	ReadBuffer	key;
	ReadBuffer	value;
	NodeList	nodes;
	
	bool		IsControllerRequest();
	bool		IsShardServerRequest();
	
	/* Master query */
	bool		GetMaster(
				 uint64_t commandID);

	/* Get config state: databases, tables, shards, quora */
	bool		GetConfigState(
				 uint64_t commandID);

	/* Quorum management */
	bool		CreateQuorum(
				 uint64_t commandID, char productionType, NodeList nodes);	
				 
	/* Database management */
	bool		CreateDatabase(
				 uint64_t commandID, char productionType, ReadBuffer& name);
	bool		RenameDatabase(
				 uint64_t commandID, uint64_t databaseID, ReadBuffer& name);
	bool		DeleteDatabase(
				 uint64_t commandID, uint64_t databaseID);
	
	/* Table management */
	bool		CreateTable(
				 uint64_t commandID, uint64_t databaseID, uint64_t quorumID, ReadBuffer& name);
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
	bool		Read(ReadBuffer& buffer);
	bool		Write(Buffer& buffer);
};

#endif
