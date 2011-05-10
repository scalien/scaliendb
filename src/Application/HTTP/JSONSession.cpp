#include "JSONSession.h"
#include "HTTPConnection.h"
#include "HTTPConsts.h"

#include <stdio.h>

void JSONSession::Init(HTTPConnection* conn_)
{
    conn = conn_;
    writer.Init(conn->GetWriteBuffer());
}

void JSONSession::SetCallbackPrefix(const ReadBuffer& jsonCallback_)
{
    writer.SetCallbackPrefix(jsonCallback_);
}

void JSONSession::Start()
{
    conn->WriteHeader(HTTP_STATUS_CODE_OK);    
    writer.Start();
}

void JSONSession::End()
{
    writer.End();
}

void JSONSession::PrintStatus(const char* status, const char* type_)
{
    writer.PrintStatus(status, type_);
    conn->Flush(true);
}

void JSONSession::PrintString(const ReadBuffer& str)
{
    writer.PrintString(str);
}

void JSONSession::PrintNumber(int64_t number)
{
    writer.PrintNumber(number);
}

void JSONSession::PrintFloatNumber(double number)
{
    writer.PrintFloatNumber(number);
}

void JSONSession::PrintBool(bool b)
{
    writer.PrintBool(b);
}

void JSONSession::PrintNull()
{
    writer.PrintNull();
}

void JSONSession::PrintObjectStart()
{
    writer.PrintObjectStart();
}

void JSONSession::PrintObjectEnd()
{
    writer.PrintObjectEnd();
}

void JSONSession::PrintArrayStart()
{
    writer.PrintArrayStart();
}

void JSONSession::PrintArrayEnd()
{
    writer.PrintArrayEnd();
}

void JSONSession::PrintColon()
{
    writer.PrintColon();
}

void JSONSession::PrintComma()
{
    writer.PrintComma();
}

void JSONSession::PrintPair(const char* s, unsigned slen, const char* v, unsigned vlen)
{
    writer.PrintPair(s, slen, v, vlen);
}

bool JSONSession::IsCommaNeeded()
{
    return writer.IsCommaNeeded();
}

void JSONSession::SetCommaNeeded(bool needed)
{
    writer.SetCommaNeeded(needed);
}

JSONBufferWriter& JSONSession::GetBufferWriter()
{
    return writer;
}
