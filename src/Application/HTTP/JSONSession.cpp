#include "JSONSession.h"
#include "HttpConn.h"
#include "HttpConsts.h"

#include <stdio.h>

void JSONSession::Init(HttpConn* conn_, const ReadBuffer& jsonCallback_)
{
	conn = conn_;
	jsonCallback.SetBuffer(jsonCallback_.GetBuffer());
	jsonCallback.SetLength(jsonCallback_.GetLength());
}

void JSONSession::PrintStatus(const char* status, const char* type_)
{
	conn->ResponseHeader(HTTP_STATUS_CODE_OK, false,
	"Content-type: application/json" HTTP_CS_CRLF);
	if (jsonCallback.GetLength())
	{
		conn->Write(jsonCallback.GetBuffer(), jsonCallback.GetLength());
		conn->Print("(");
	}
	conn->Print("{\"status\":\"");
	conn->Print(status);
	if (type_)
	{
		conn->Print("\",\"type\":\"");
		conn->Print(type_);
	}
	conn->Print("\"}");

	if (jsonCallback.GetLength())
		conn->Print(")");
	
	conn->Flush(true);
}

void JSONSession::PrintString(const char *s, unsigned len)
{
	conn->Write("\"", 1);
	for (unsigned i = 0; i < len; i++)
	{
		if (s[i] == '"')
			conn->Write("\\", 1);
		conn->Write(s + i, 1);
	}
	conn->Write("\"", 1);
}

void JSONSession::PrintNumber(double number)
{
	char		buffer[32];
	unsigned	len;
	
	len = snprintf(buffer, sizeof(buffer), "%lf", number);
	conn->Write(buffer, len);
}

void JSONSession::PrintBool(bool b)
{
	if (b)
		conn->Write("true", 4);
	else
		conn->Write("false", 5);
}

void JSONSession::PrintNull()
{
	conn->Write("null", 4);
}

void JSONSession::PrintObjectStart(const char* /*name*/, unsigned /*len*/)
{
	// TODO
}

void JSONSession::PrintObjectEnd()
{
	// TODO
}
