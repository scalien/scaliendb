#include "SDBPRequestMessage.h"

bool SDBPRequestMessage::Read(ReadBuffer& buffer)
{
	int			read;
	unsigned	i, numNodes;
	uint64_t	nodeID;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		/* Master query */
		case CLIENTREQUEST_GET_MASTER:
			read = buffer.Readf("%c:%U",
			 &request->type, &request->commandID);
			break;

		/* Get config state: databases, tables, shards, quora */
		case CLIENTREQUEST_GET_CONFIG_STATE:
			read = buffer.Readf("%c:%U",
			 &request->type, &request->commandID);
			break;

		/* Quorum management */
		case CLIENTREQUEST_CREATE_QUORUM:
			read = buffer.Readf("%c:%U:%c:%U",
			 &request->type, &request->commandID, &request->productionType,
			 numNodes);
			if (read < 0 || read == (signed)buffer.GetLength())
				return false;
			buffer.Advance(read);
			for (i = 0; i < numNodes; i++)
			{
				read = buffer.Readf(":%U", nodeID);
				if (read < 0)
					return false;
				buffer.Advance(read);
				request->nodes.Append(nodeID);
			}
			if (buffer.GetLength() == 0)
				return true;
			else
				return false;
			break;

		/* Database management */
		case CLIENTREQUEST_CREATE_DATABASE:
			read = buffer.Readf("%c:%U:%c:%#B",
			 &request->type, &request->commandID, &request->productionType,
			 &request->name);
			break;
		case CLIENTREQUEST_RENAME_DATABASE:
			read = buffer.Readf("%c:%U:%U:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->name);
			break;
		case CLIENTREQUEST_DELETE_DATABASE:
			read = buffer.Readf("%c:%U:%U",
			 &request->type, &request->commandID, &request->databaseID);
			break;
			
		/* Table management */
		case CLIENTREQUEST_CREATE_TABLE:
			read = buffer.Readf("%c:%U:%U:%U:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->quorumID, &request->name);
			break;
		case CLIENTREQUEST_RENAME_TABLE:
			read = buffer.Readf("%c:%U:%U:%U:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->tableID, &request->name);
			break;
		case CLIENTREQUEST_DELETE_TABLE:
			read = buffer.Readf("%c:%U:%U:%U",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->tableID);
			break;
			
		/* Data operations */
		case CLIENTREQUEST_GET:
			read = buffer.Readf("%c:%U:%U:%U:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->tableID, &request->key);
			break;
		case CLIENTREQUEST_SET:
			read = buffer.Readf("%c:%U:%U:%U:%#B:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->tableID, &request->key, &request->value);
			break;
		case CLIENTREQUEST_DELETE:
			read = buffer.Readf("%c:%U:%U:%U:%#B",
			 &request->type, &request->commandID, &request->databaseID,
			 &request->tableID, &request->key);
			break;
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);
}

bool SDBPRequestMessage::Write(Buffer& buffer)
{
	uint64_t*	it;
	
	switch (request->type)
	{
		/* Master query */
		case CLIENTREQUEST_GET_MASTER:
			buffer.Writef("%c:%U",
			 request->type, request->commandID);
			return true;

		/* Get config state: databases, tables, shards, quora */
		case CLIENTREQUEST_GET_CONFIG_STATE:
			buffer.Writef("%c:%U",
			 request->type, request->commandID);
			return true;

		/* Quorum management */
		case CLIENTREQUEST_CREATE_QUORUM:
			buffer.Writef("%c:%U:%c:%U",
			 request->type, request->commandID, request->productionType,
			 request->nodes.GetLength());
			for (it = request->nodes.First(); it != NULL; it = request->nodes.Next(it))
				buffer.Appendf(":%U", *it);

		/* Database management */
		case CLIENTREQUEST_CREATE_DATABASE:
			buffer.Writef("%c:%U:%c:%#B",
			 request->type, request->commandID, request->productionType,
			 &request->name);
			return true;
		case CLIENTREQUEST_RENAME_DATABASE:
			buffer.Writef("%c:%U:%U:%#B",
			 request->type, request->commandID, request->databaseID,
			 &request->name);
			return true;
		case CLIENTREQUEST_DELETE_DATABASE:
			buffer.Writef("%c:%U:%U",
			 request->type, request->commandID, request->databaseID);
			return true;

		/* Table management */
		case CLIENTREQUEST_CREATE_TABLE:
			buffer.Writef("%c:%U:%U:%U:%#B",
			 request->type, request->commandID, request->databaseID,
			 request->quorumID, &request->name);
			return true;
		case CLIENTREQUEST_RENAME_TABLE:
			buffer.Writef("%c:%U:%U:%U:%#B",
			 request->type, request->commandID, request->databaseID,
			 request->tableID, &request->name);
			return true;
		case CLIENTREQUEST_DELETE_TABLE:
			buffer.Writef("%c:%U:%U:%U",
			 request->type, request->commandID, request->databaseID,
			 request->tableID);
			return true;

		/* Data operations */
		case CLIENTREQUEST_GET:
			buffer.Writef("%c:%U:%U:%U:%#B",
			 request->type, request->commandID, request->databaseID,
			 request->tableID, &request->key);
			return true;
		case CLIENTREQUEST_SET:
			buffer.Writef("%c:%U:%U:%U:%#B:%#B",
			 request->type, request->commandID, request->databaseID,
			 request->tableID, &request->key, &request->value);
			return true;
		case CLIENTREQUEST_DELETE:
			buffer.Writef("%c:%U:%U:%U:%#B",
			 request->type, request->commandID, request->databaseID,
			 request->tableID, &request->key);
			return true;
		
		default:
			return false;
	}
	
	return true;
}
