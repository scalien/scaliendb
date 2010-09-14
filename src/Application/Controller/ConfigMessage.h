#ifndef CONFIGMESSAGE_H
#define CONFIGMESSAGE_H

#include "System/Platform.h"
#include "System/IO/Endpoint.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/List.h"
#include "Framework/Messaging/Message.h"

#define CONFIG_REGISTER_SHARDSERVER		'S'
#define CONFIG_CREATE_QUORUM			'Q'
#define CONFIG_INCREASE_QUORUM			'P'
#define CONFIG_DECREASE_QUORUM			'M'

#define CONFIG_CREATE_DATABASE			'C'
#define CONFIG_RENAME_DATABASE			'R'
#define CONFIG_DELETE_DATABASE			'D'

#define CONFIG_CREATE_TABLE				'c'
#define CONFIG_RENAME_TABLE				'r'
#define CONFIG_DELETE_TABLE				'd'

/*
===============================================================================

 ConfigMessage

===============================================================================
*/

class ConfigMessage : public Message
{
public:
	typedef List<uint64_t> NodeList;

	/* Variables */
	char			type;
	char			productionType;
	uint64_t		nodeID;
	uint64_t		quorumID;
	uint64_t		databaseID;
	uint64_t		tableID;
	uint64_t		shardID;
	ReadBuffer		name;
	Endpoint		endpoint;
	NodeList		nodes;
	
	ConfigMessage*	prev;
	ConfigMessage*	next;

	/* Cluster management */
	bool			RegisterShardServer(
					 uint64_t nodeID, Endpoint& endpoint);
	bool			CreateQuorum(
					 uint64_t quorumID, char productionType, NodeList& nodes);
	bool			IncreaseQuorum(
					 uint64_t shardID, uint64_t nodeID);
	bool			DecreaseQuorum(
					 uint64_t shardID, uint64_t nodeID);

	/* Database management*/
	bool			CreateDatabase(
					 uint64_t databaseID, ReadBuffer& name);
	bool			RenameDatabase(
					 uint64_t databaseID, ReadBuffer& name);
	bool			DeleteDatabase(
					 uint64_t databaseID);

	/* Table management*/
	bool			CreateTable(
					 uint64_t databaseID, uint64_t tableID, uint64_t shardID,
					 uint64_t quorumID, ReadBuffer& name);
	bool			RenameTable(
					 uint64_t databaseID, uint64_t tableID, ReadBuffer& name);
	bool			DeleteTable(
					 uint64_t databaseID, uint64_t tableID);
	
	/* Serialization */
	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
