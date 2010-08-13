#include "HttpConn.h"
#include "Version.h"
#include "HttpServer.h"
#include "HttpConsts.h"
#include "Framework/Database/Database.h"

#define CS_CR				"\015"
#define CS_LF				"\012"
#define CS_CRLF				CS_CR CS_LF
#define MSG_NOT_FOUND		"Not found"

HttpConn::HttpConn()
{
	server = NULL;
}

void HttpConn::Init(HttpServer* server_)
{
	TCPConnection::InitConnected();
	
	server = server_;
	
	request.Init();
	socket.GetEndpoint(endpoint);
	
	// HACK
	closeAfterSend = false;
}

void HttpConn::SetOnClose(const Callable& callable)
{
	onCloseCallback = callable;
}

void HttpConn::OnRead()
{
	Log_Trace();
	int len;
	
	len = Parse(tcpread.buffer->GetBuffer(), tcpread.buffer->GetLength());

	if (len == 0)
	{
		if (tcpread.buffer->GetSize() > 1*MB)
		{
			Flush(); // closes the connection
			return;
		}
			
		tcpread.offset = tcpread.buffer->GetLength();
		tcpread.buffer->Allocate(2 * tcpread.buffer->GetLength());
		IOProcessor::Add(&tcpread);
	}
}


void HttpConn::OnClose()
{
	Log_Trace();
	
	Close();
	request.Free();
	if (closeAfterSend)
	{
		if (onCloseCallback.IsSet())
			Call(onCloseCallback);
		else
			server->DeleteConn(this);
	}
}


void HttpConn::OnWrite()
{
	Log_Trace();
	TCPConnection::OnWrite();
	if (closeAfterSend && !tcpwrite.active)
		OnClose();
}


void HttpConn::Print(const char* s)
{
	Buffer*			buffer;
	
	buffer = writer->AcquireBuffer(strlen(s));
	buffer->Write(s, strlen(s));
	writer->WritePooled(buffer);
}

void HttpConn::Write(const char* s, unsigned length)
{
	Buffer*			buffer;
	
	buffer = writer->AcquireBuffer(length);
	buffer->Write(s, length);
	writer->WritePooled(buffer);
}

int HttpConn::Parse(char* buf, int len)
{
	int pos;
	
	pos = request.Parse(buf, len);
	if (pos <= 0)
		return pos;
	
	if (server->HandleRequest(this, request))
		return -1;
	
	Response(HTTP_STATUS_CODE_NOT_FOUND, MSG_NOT_FOUND, sizeof(MSG_NOT_FOUND) - 1);
	closeAfterSend = true;
		
	return -1;
}


const char* HttpConn::Status(int code)
{
	if (code == HTTP_STATUS_CODE_OK)
		return "OK";
	if (code == HTTP_STATUS_CODE_NOT_FOUND)
		return "Not found";
	
	return "";
}

void HttpConn::Response(int code, const char* data,
int len, bool close, const char* header)
{	
	Buffer		*httpHeader;
	unsigned	size;

	Log_Message("[%s] HTTP: %.*s %.*s %d %d", endpoint.ToString(),
				request.line.method.GetLength(), request.line.method.GetBuffer(), 
				request.line.uri.GetLength(), request.line.uri.GetBuffer(),
				code, len);

	httpHeader = writer->AcquireBuffer();
	size = httpHeader->Writef(
				"%R %d %s" CS_CRLF
				"Accept-Range: bytes" CS_CRLF
				"Content-Length: %d" CS_CRLF
				"Cache-Control: no-cache" CS_CRLF
				"%s"
				"%s"
				CS_CRLF
				, 
				&request.line.version,
				code, Status(code),
				len,
				close ? "Connection: close" CS_CRLF : "",
				header ? header : "");

	httpHeader->Append(data, len);
	
	Log_Trace("buffer = %.*s", P(httpHeader));
	writer->WritePooled(httpHeader);
	//Write(data, len);
	Flush(true);
}

void HttpConn::ResponseHeader(int code, bool close, const char* header)
{
	Buffer*		httpHeader;

	Log_Message("[%s] HTTP: %.*s %.*s %d ?",
				endpoint.ToString(), 
				request.line.method.GetLength(), request.line.method.GetBuffer(),
				request.line.uri.GetLength(), request.line.uri.GetBuffer(),
				code);

	httpHeader = writer->AcquireBuffer();
	httpHeader->Writef(
				"%R %d %s" CS_CRLF
				"Cache-Control: no-cache" CS_CRLF
				"%s"
				"%s"
				CS_CRLF
				, 
				&request.line.version,
				code, Status(code),
				close ? "Connection: close" CS_CRLF : "",
				header ? header : "");

	writer->WritePooled(httpHeader);
	Flush();
}

void HttpConn::Flush(bool closeAfterSend_)
{
	writer->Flush();
	closeAfterSend = closeAfterSend_;
}
