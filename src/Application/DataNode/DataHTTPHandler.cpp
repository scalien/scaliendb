#include "DataHTTPHandler.h"
#include "DataChunkContext.h"
#include "Application/HTTP/UrlParam.h"
#include "Framework/Replication/ReplicationConfig.h"

#define HTTP_GET_PARAM(params, name, var) \
if (!params.GetNamed(name, sizeof("" name) - 1, var)) { return NULL; }

void DataHTTPHandler::Init(DataNode* dataNode_)
{
	dataNode = dataNode_;
}

bool DataHTTPHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
	char*		pos;
	char*		qmark;
	unsigned	cmdlen;
	UrlParam	params;

	pos = (char*) request.line.uri.GetBuffer();
	if (*pos == '/') 
		pos++;
	
	qmark = FindInBuffer(pos, request.line.uri.GetLength() - 1, '?');
	if (qmark)
	{
		params.Init(qmark + 1, request.line.uri.GetLength() - 2, '&');
		cmdlen = qmark - pos;
	}
	else
		cmdlen = request.line.uri.GetLength() - 1;

	return ProcessCommand(conn, pos, cmdlen, params);
}

void DataHTTPHandler::OnComplete(DataMessage* msg, bool status)
{
	HTTPConnection*	conn;
	Buffer		buffer;
	
	conn = (HTTPConnection*) msg->ptr;

	if (msg->type == DATAMESSAGE_SET)
	{
		if (status)
			buffer.Write("OK");
		else
			buffer.Write("FAILED");
	}
	else if (msg->type == DATAMESSAGE_GET)
	{
		if (status)
			buffer.Write(msg->value);
		else
			buffer.Write("FAILED");
	}
	else
		ASSERT_FAIL();

	conn->Response(HTTP_STATUS_CODE_OK, buffer.GetBuffer(), buffer.GetLength());
}

bool DataHTTPHandler::MatchString(const char* s1, unsigned len1, const char* s2, unsigned len2)
{
	if (len1 != len2)
		return false;
		
	return (strncmp(s1, s2, len2) == 0);
}

#define STR_AND_LEN(s) s, strlen(s)

bool DataHTTPHandler::ProcessCommand(HTTPConnection* conn, const char* cmd, unsigned cmdlen, UrlParam& params)
{
	Buffer			buffer;

	Log_Trace();

	if (MatchString(cmd, cmdlen, STR_AND_LEN("")))
	{
		PrintHello(conn);
		return true;
	}
	
	if (MatchString(cmd, cmdlen, STR_AND_LEN("get"))) 
	{
		ReadBuffer	key;
		
		Log_Trace("GET");
	
		HTTP_GET_PARAM(params, "key", key);

		DataChunkContext*	context;
		context = (DataChunkContext*) REPLICATION_CONFIG->GetContext(1); // TODO: hack
		context->Get(key, (void*) conn);
		
		return true;
	}
	else if (MatchString(cmd, cmdlen, STR_AND_LEN("set")))
	{
		ReadBuffer	key;
		ReadBuffer	value;

		Log_Trace("SET");
		
		HTTP_GET_PARAM(params, "key", key);
		HTTP_GET_PARAM(params, "value", value);

		DataChunkContext*	context;
		context = (DataChunkContext*) REPLICATION_CONFIG->GetContext(1); // TODO: hack
		context->Set(key, value, (void*) conn);
		
		return true;
	}
	else
		return false;
}

void DataHTTPHandler::PrintHello(HTTPConnection* conn)
{
	Buffer			buffer;
	QuorumContext*	context;

	context = REPLICATION_CONFIG->GetContext(1); // TODO: hack
	
	if (context->IsLeaderKnown())
	{
		buffer.Writef("Self: %U\nPrimary: %U\nPaxosID: %U\nChunkID: 1\n",
		 REPLICATION_CONFIG->GetNodeID(),
		 context->GetLeader(),
		 context->GetPaxosID());
	}
	else
	{
		buffer.Writef("Self: %U\nNo primary\nPaxosID: %U\nChunkID: 1\n", 
		 REPLICATION_CONFIG->GetNodeID(),
		 context->GetPaxosID());
	}

	conn->Response(HTTP_STATUS_CODE_OK, buffer.GetBuffer(), buffer.GetLength());
}
