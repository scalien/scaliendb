#include "DataNode.h"
#include "System/Common.h"
#include "System/Platform.h"
#include "Framework/Replication/ReplicationManager.h"
#include "Application/HTTP/HttpConsts.h"

#define GET_NAMED_PARAM(params, name, var) \
if (!params.GetNamed(name, sizeof("" name) - 1, var)) { return NULL; }

void DataNode::Init(Table* table_)
{
	table = table_;
	
	if (!table->Get(NULL, "#nodeID", nodeID))
	{
		nodeID = RandomInt(0, 64000); //gen_uuid();
		Log_Trace("%" PRIu64, nodeID);
		table->Set(NULL, "#nodeID", nodeID);
	}
	RMAN->SetNodeID(nodeID);
	Log_Trace("nodeID = %" PRIu64, nodeID);
}

bool DataNode::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	char*		pos;
	char*		qmark;
	unsigned	cmdlen;
	UrlParam	params;

	pos = (char*) request.line.uri.GetBuffer();
	if (*pos == '/') 
		pos++;
	
//	pos = ParseType(pos);
	qmark = strnchr(pos, '?', request.line.uriLen - 1);
	if (qmark)
	{
		params.Init(qmark + 1, request.line.uriLen - 2, '&');
//		if (type == JSON) 
//			GET_NAMED_OPT_PARAM(params, "callback", jsonCallback);
			
		cmdlen = qmark - pos;
	}
	else
		cmdlen = request.line.uriLen - 1;

	return ProcessCommand(conn, pos, cmdlen, params);
}

bool DataNode::MatchString(const char* s1, unsigned len1, const char* s2, unsigned len2)
{
	if (len1 != len2)
		return false;
		
	return (strncmp(s1, s2, len2) == 0);
}


#define STR_AND_LEN(s) s, strlen(s)

bool DataNode::ProcessCommand(HttpConn* conn, const char* cmd, unsigned cmdlen, const UrlParam& params)
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
	
		GET_NAMED_PARAM(params, "key", key);

		DataChunkContext*	context;
		context = (DataChunkContext*) RMAN->GetContext(1); // TODO: hack
		context->Get(key, (void*) conn);
		
		return true;
	}
	else if (MatchString(cmd, cmdlen, STR_AND_LEN("set")))
	{
		ReadBuffer	key;
		ReadBuffer	value;

		Log_Trace("SET");
		
		GET_NAMED_PARAM(params, "key", key);
		GET_NAMED_PARAM(params, "value", value);

		DataChunkContext*	context;
		context = (DataChunkContext*) RMAN->GetContext(1); // TODO: hack
		context->Set(key, value, (void*) conn);
		
		return true;
	}
	else
		return false;
}

void DataNode::PrintHello(HttpConn* conn)
{
	Buffer			buffer;
	QuorumContext*	context;

	context = RMAN->GetContext(1); // TODO: hack
	
	if (context->IsLeaderKnown())
	{
		buffer.Writef("ChunkID: 1\nPrimary: %U\nSelf: %U\nPaxosID: %U\n", 
		 context->GetLeader(),
		 RMAN->GetNodeID(),
		 context->GetPaxosID());
	}
	else
	{
		buffer.Writef("ChunkID: 1\nNo primary, PaxosID: %U\n", context->GetPaxosID());
	}

	conn->Response(HTTP_STATUS_CODE_OK, buffer.GetBuffer(), buffer.GetLength());
	//conn->Flush(true);
}

void DataNode::OnComplete(DataMessage* msg, bool status)
{
	HttpConn*	conn;
	Buffer		buffer;
	
	conn = (HttpConn*) msg->ptr;

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
	//conn->Flush(true);
}
