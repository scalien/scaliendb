#include "HTTPControllerSession.h"
#include "Controller.h"

#include "Application/HTTP/UrlParam.h"

#define MSG_FAIL			"Unable to process your request at this time"

#define MATCH_COMMAND(cmd, csl) \
	MEMCMP(cmd.GetBuffer(), cmd.GetLength(), csl, sizeof(csl) - 1)

#define GET_NAMED_PARAM(params, name, var) \
	if (!params.GetNamed(name, sizeof("" name) - 1, var)) { return NULL; }

#define GET_NAMED_U64_PARAM(params, name, var) \
	{ \
		ReadBuffer	tmp; \
		unsigned	nread; \
		if (!params.GetNamed(name, sizeof("" name) - 1, tmp)) \
			return NULL; \
		var = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread); \
		if (nread != tmp.GetLength()) \
			return NULL; \
	}

void HTTPControllerSession::SetConnection(HTTPConnection* conn_)
{
	conn = conn_;
}

void HTTPControllerSession::SetController(Controller* controller_)
{
	controller = controller_;
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

bool HTTPControllerSession::HandleRequest(HTTPRequest& request)
{
	char*		qmark;
	UrlParam	params;
	ReadBuffer	rb;
	ReadBuffer	cmd;
	
	rb = request.line.uri;
	if (rb.GetCharAt(0) == '/')
		rb.Advance(1);
	
	ParseType(rb);
	cmd = rb;
	qmark = FindInBuffer(rb.GetBuffer(), rb.GetLength() - 1, '?');
	if (qmark)
	{
		rb.Advance(qmark - rb.GetBuffer() + 1);
		params.Init(rb.GetBuffer(), rb.GetLength(), '&');
		cmd.SetLength(qmark - cmd.GetBuffer());
	}
		
	return ProcessCommand(cmd, params);
}

void HTTPControllerSession::PrintStatus()
{
	// TODO: print something
}

void HTTPControllerSession::ResponseFail()
{
	conn->Response(HTTP_STATUS_CODE_OK, MSG_FAIL, sizeof(MSG_FAIL) - 1);
}

void HTTPControllerSession::ParseType(ReadBuffer& rb)
{
	const char	JSON_PREFIX[] = "json/";
	const char	HTML_PREFIX[] = "html/";
	
	type = PLAIN;
	
	if (MATCH_COMMAND(rb, JSON_PREFIX))
	{
		type = JSON;
		rb.Advance(sizeof(JSON_PREFIX) - 1);
	}
	else if (MATCH_COMMAND(rb, HTML_PREFIX))
	{
		type = HTML;
		rb.Advance(sizeof(HTML_PREFIX) - 1);
	}
}

bool HTTPControllerSession::ProcessCommand(ReadBuffer& cmd, UrlParam& params)
{
	ConfigMessage*	message;
	
	if (MATCH_COMMAND(cmd, ""))
	{
		PrintStatus();
		return true;
	}
	else if (MATCH_COMMAND(cmd, "getmaster"))
	{
		ProcessGetMaster();
		return true;
	}

	message = ProcessControllerCommand(cmd, params);
	if (!message)
		return false;

	if (!controller->ProcessClientCommand(this, *message))
	{
		ResponseFail();
		delete message;
		return true;
	}
	
	return true;
}

void HTTPControllerSession::ProcessGetMaster()
{
	// TODO: print master
}

ConfigMessage* HTTPControllerSession::ProcessControllerCommand(ReadBuffer& cmd, UrlParam& params)
{
	if (MATCH_COMMAND(cmd, "createquorum"))
		return ProcessCreateQuorum(params);
	if (MATCH_COMMAND(cmd, "increasequorum"))
		return ProcessIncreaseQuorum(params);
	if (MATCH_COMMAND(cmd, "decreasequorum"))
		return ProcessDecreaseQuorum(params);
	if (MATCH_COMMAND(cmd, "createdatabase"))
		return ProcessCreateDatabase(params);
	if (MATCH_COMMAND(cmd, "renamedatabase"))
		return ProcessRenameDatabase(params);
	if (MATCH_COMMAND(cmd, "deletedatabase"))
		return ProcessDeleteDatabase(params);
	if (MATCH_COMMAND(cmd, "createtable"))
		return ProcessCreateTable(params);
	if (MATCH_COMMAND(cmd, "renametable"))
		return ProcessRenameTable(params);
	if (MATCH_COMMAND(cmd, "deletetable"))
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
	
	GET_NAMED_U64_PARAM(params, "quorumID", quorumID);
	GET_NAMED_PARAM(params, "productionType", tmp);
	if (tmp.GetLength() > 1)
		return NULL;

	// TODO: validate values for productionType
	productionType = tmp.GetCharAt(0);
	
	GET_NAMED_PARAM(params, "nodes", tmp);
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
	
	GET_NAMED_U64_PARAM(params, "shardID", shardID);
	GET_NAMED_U64_PARAM(params, "nodeID", nodeID);

	message = new ConfigMessage;
	message->IncreaseQuorum(shardID, nodeID);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessDecreaseQuorum(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		shardID;
	uint64_t		nodeID;
	
	GET_NAMED_U64_PARAM(params, "shardID", shardID);
	GET_NAMED_U64_PARAM(params, "nodeID", nodeID);

	message = new ConfigMessage;
	message->DecreaseQuorum(shardID, nodeID);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessCreateDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	ReadBuffer		name;
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);
	GET_NAMED_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->CreateDatabase(databaseID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessRenameDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	ReadBuffer		name;
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);
	GET_NAMED_PARAM(params, "name", name);

	message = new ConfigMessage;
	message->RenameDatabase(databaseID, name);

	return message;
}

ConfigMessage* HTTPControllerSession::ProcessDeleteDatabase(UrlParam& params)
{
	ConfigMessage*	message;
	uint64_t		databaseID;
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);

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
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);
	GET_NAMED_U64_PARAM(params, "tableID", tableID);
	GET_NAMED_U64_PARAM(params, "shardID", shardID);
	GET_NAMED_U64_PARAM(params, "quorumID", quorumID);
	GET_NAMED_PARAM(params, "name", name);

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
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);
	GET_NAMED_U64_PARAM(params, "tableID", tableID);
	GET_NAMED_PARAM(params, "name", name);

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
	
	GET_NAMED_U64_PARAM(params, "databaseID", databaseID);
	GET_NAMED_U64_PARAM(params, "tableID", tableID);

	message = new ConfigMessage;
	message->DeleteTable(databaseID, tableID);

	return message;
}
