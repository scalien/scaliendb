#include "HTTPSession.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/HTTP/HTTPRequest.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/HTTP/HTTPConsts.h"

#define MSG_FAIL	 "Unable to process your request at this time"

HTTPSession::HTTPSession()
{
	headerSent = false;
	conn = NULL;
	type = PLAIN;
}

void HTTPSession::SetConnection(HTTPConnection* conn_)
{
	conn = conn_;
	json.Init(conn);
}

bool HTTPSession::ParseRequest(HTTPRequest& request, ReadBuffer& cmd, UrlParam& params)
{
	char*		qmark;
	ReadBuffer	rb;
	ReadBuffer	jsonCallback;
	
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
		
		if (type == JSON)
		{
			HTTP_GET_OPT_PARAM(params, "callback", jsonCallback);
			json.SetCallbackPrefix(jsonCallback);
		}
	}
	
	return true;
}

void HTTPSession::ParseType(ReadBuffer& rb)
{
	const char	JSON_PREFIX[] = "json/";
	const char	HTML_PREFIX[] = "html/";
	
	type = PLAIN;
	
	if (HTTP_MATCH_COMMAND(rb, JSON_PREFIX))
	{
		type = JSON;
		rb.Advance(sizeof(JSON_PREFIX) - 1);
	}
	else if (HTTP_MATCH_COMMAND(rb, HTML_PREFIX))
	{
		type = HTML;
		rb.Advance(sizeof(HTML_PREFIX) - 1);
	}
}

void HTTPSession::ResponseFail()
{
	assert(headerSent == false);
	
	if (type == JSON)
		json.PrintStatus("error", MSG_FAIL);
	else
		conn->Response(HTTP_STATUS_CODE_OK, MSG_FAIL, sizeof(MSG_FAIL) - 1);
}

void HTTPSession::PrintLine(ReadBuffer& line)
{
	if (!headerSent)
	{
		if (type == JSON)
			json.Start();
		else
			conn->ResponseHeader(HTTP_STATUS_CODE_OK, false);

		headerSent = true;
	}
	
	if (type == JSON)
		json.PrintString(line.GetBuffer(), line.GetLength());
	else
		conn->Write(line.GetBuffer(), line.GetLength());
}

void HTTPSession::PrintPair(const ReadBuffer& key, const ReadBuffer& value)
{
	if (!headerSent)
	{
		if (type == JSON)
			json.Start();
		else
			conn->ResponseHeader(HTTP_STATUS_CODE_OK, false);

		headerSent = true;
	}

	if (type == JSON)
		json.PrintPair(key.GetBuffer(), key.GetLength(), value.GetBuffer(), value.GetLength());
	else
	{
		conn->Write(key.GetBuffer(), key.GetLength());
		conn->Print(": ");
		conn->Write(value.GetBuffer(), value.GetLength());
		conn->Print("\n");
	}
}

void HTTPSession::PrintPair(const char* key, const char* value)
{
	PrintPair(ReadBuffer(key), ReadBuffer(value));
}

void HTTPSession::Flush()
{
	if (type == JSON)
		json.End();
	
	conn->Flush(true);
}

