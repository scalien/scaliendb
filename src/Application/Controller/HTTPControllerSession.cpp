#include "HTTPControllerSession.h"
#include "Controller.h"
#include "Application/HTTP/UrlParam.h"
#include "Version.h"

void HTTPControllerSession::SetController(Controller* controller_)
{
	controller = controller_;
}

void HTTPControllerSession::SetConnection(HTTPConnection* conn_)
{
	session.SetConnection(conn_);
}

bool HTTPControllerSession::HandleRequest(HTTPRequest& request)
{
	ReadBuffer	cmd;
	UrlParam	params;
	
	session.ParseRequest(request, cmd, params);
	return ProcessCommand(cmd, params);
}

void HTTPControllerSession::OnComplete(Message* message, bool status)
{
	ConfigMessage*	configMessage;
	
	configMessage = (ConfigMessage*) message;
	// TODO:
	
	delete configMessage;
}

bool HTTPControllerSession::IsActive()
{
	return true;
}

void HTTPControllerSession::PrintStatus()
{
	// TODO: print something
	Buffer		buf;

	session.PrintPair("ScalienDB", "Controller");
	session.PrintPair("Version", VERSION_STRING);

	buf.Writef("%d", (int) controller->GetMaster());
	buf.NullTerminate();
	session.PrintPair("Master", buf.GetBuffer());
	
	session.Flush();
}

bool HTTPControllerSession::ProcessCommand(ReadBuffer& cmd, UrlParam& params)
{
	ConfigMessage*	message;
	
	if (HTTP_MATCH_COMMAND(cmd, ""))
	{
		PrintStatus();
		return true;
	}
	else if (HTTP_MATCH_COMMAND(cmd, "getmaster"))
	{
		ProcessGetMaster();
		return true;
	}

	message = ProcessControllerCommand(cmd, params);
	if (!message)
		return false;

	if (!controller->ProcessClientCommand(this, *message))
	{
		session.ResponseFail();
		delete message;
		return true;
	}
	
	return true;
}

void HTTPControllerSession::ProcessGetMaster()
{
}

ConfigMessage* HTTPControllerSession::ProcessControllerCommand(ReadBuffer& cmd, UrlParam& params)
{
	if (HTTP_MATCH_COMMAND(cmd, "createquorum"))
		return ProcessCreateQuorum(params);
	if (HTTP_MATCH_COMMAND(cmd, "increasequorum"))
		return ProcessIncreaseQuorum(params);
	if (HTTP_MATCH_COMMAND(cmd, "decreasequorum"))
		return ProcessDecreaseQuorum(params);
	if (HTTP_MATCH_COMMAND(cmd, "createdatabase"))
		return ProcessCreateDatabase(params);
	if (HTTP_MATCH_COMMAND(cmd, "renamedatabase"))
		return ProcessRenameDatabase(params);
	if (HTTP_MATCH_COMMAND(cmd, "deletedatabase"))
		return ProcessDeleteDatabase(params);
	if (HTTP_MATCH_COMMAND(cmd, "createtable"))
		return ProcessCreateTable(params);
	if (HTTP_MATCH_COMMAND(cmd, "renametable"))
		return ProcessRenameTable(params);
	if (HTTP_MATCH_COMMAND(cmd, "deletetable"))
		return ProcessDeleteTable(params);
	
	return NULL;
}

ConfigMessage* HTTPControllerSession::ProcessCreateQuorum(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		quorumID;
	char			productionType;
	List<uint64_t>	nodes;
	ReadBuffer		tmp;
	char*			next;
	unsigned		nread;
	uint64_t		nodeID;
	
	HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
	HTTP_GET_PARAM(params, "productionType", tmp);
	if (tmp.GetLength() > 1)
		return NULL;

	// TODO: validate values for productionType
	productionType = tmp.GetCharAt(0);
	
	// parse comma separated nodeID values
	HTTP_GET_PARAM(params, "nodes", tmp);
	while ((next = FindInBuffer(tmp.GetBuffer(), tmp.GetLength(), ',')) != NULL)
	{
		nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
		if (nread != next - tmp.GetBuffer())
			return NULL;
		next++;
		tmp.Advance(next - tmp.GetBuffer());
		nodes.Append(nodeID);
	}
	
	nodeID = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread);
	if (nread != tmp.GetLength())
		return NULL;
	nodes.Append(nodeID);

	message = new ConfigMessage;
	message->CreateQuorum(quorumID, productionType, nodes);
	
	return message;
}

ConfigMessage* HTTPControllerSession::ProcessIncreaseQuorum(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		shardID;
	uint64_t		nodeID;
	
	HTTP_GET_U64_PARAM(params, "shardID", shardID);
	HTTP_GET_U64_PARAM(params, "nodeID", nodeID);

	message = new ConfigMessage;
	message->IncreaseQuorum(shardID, nodeID);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessDecreaseQuorum(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		shardID;
	uint64_t		nodeID;
	
	HTTP_GET_U64_PARAM(params, "shardID", shardID);
	HTTP_GET_U64_PARAM(params, "nodeID", nodeID);

	message = new ConfigMessage;
	message->DecreaseQuorum(shardID, nodeID);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessCreateDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	ReadBuffer		name;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
	HTTP_GET_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->CreateDatabase(databaseID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessRenameDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	ReadBuffer		name;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
	HTTP_GET_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->RenameDatabase(databaseID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessDeleteDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);

	message = new ConfigMessage;
	message->DeleteDatabase(databaseID);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessCreateTable(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	uint64_t		tableID;
	uint64_t		shardID;
	uint64_t		quorumID;
	ReadBuffer		name;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
	HTTP_GET_U64_PARAM(params, "tableID", tableID);
	HTTP_GET_U64_PARAM(params, "shardID", shardID);
	HTTP_GET_U64_PARAM(params, "quorumID", quorumID);
	HTTP_GET_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->CreateTable(databaseID, tableID, shardID, quorumID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessRenameTable(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	uint64_t		tableID;
	ReadBuffer		name;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
	HTTP_GET_U64_PARAM(params, "tableID", tableID);
	HTTP_GET_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->RenameTable(databaseID, tableID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessDeleteTable(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	uint64_t		tableID;
	ReadBuffer		name;
	
	HTTP_GET_U64_PARAM(params, "databaseID", databaseID);
	HTTP_GET_U64_PARAM(params, "tableID", tableID);

	message = new ConfigMessage;
	message->DeleteTable(databaseID, tableID);

	return message;
}
