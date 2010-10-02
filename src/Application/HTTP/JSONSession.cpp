#include "JSONSession.h"
#include "HTTPConnection.h"
#include "HTTPConsts.h"

#include <stdio.h>

void JSONSession::Init(HTTPConnection* conn_)
{
    conn = conn_;
    jsonCallback.SetLength(0);
    depth = 0;
    depthComma = 0;
}

void JSONSession::SetCallbackPrefix(const ReadBuffer& jsonCallback_)
{
    jsonCallback = jsonCallback_;
}

void JSONSession::Start()
{
    conn->ResponseHeader(HTTP_STATUS_CODE_OK, false,
     "Content-type: application/json" HTTP_CS_CRLF);
    
    if (jsonCallback.GetLength())
    {
        conn->Write(jsonCallback.GetBuffer(), jsonCallback.GetLength());
        conn->Print("(");
    }
    
    PrintObjectStart();
}

void JSONSession::End()
{
    PrintObjectEnd();
    if (jsonCallback.GetLength())
        conn->Print(")");
}

void JSONSession::PrintStatus(const char* status, const char* type_)
{
    Start();
    
    conn->Print("{\"status\":\"");
    conn->Print(status);
    if (type_)
    {
        conn->Print("\",\"type\":\"");
        conn->Print(type_);
    }
    conn->Print("\"}");

    End();
    
    conn->Flush(true);
}

void JSONSession::PrintString(const ReadBuffer& str)
{
    const char* s;
    
    s = str.GetBuffer();
    conn->Write("\"", 1);
    for (unsigned i = 0; i < str.GetLength(); i++)
    {
        if (s[i] == '"')
            conn->Write("\\", 1);
        conn->Write(s + i, 1);
    }
    conn->Write("\"", 1);

    SetCommaNeeded(true);
}

void JSONSession::PrintNumber(int64_t number)
{
    char        buffer[32];
    unsigned    len;
    
    len = snprintf(buffer, sizeof(buffer), "%" PRIi64, number);
    conn->Write(buffer, len);

    SetCommaNeeded(true);
}

void JSONSession::PrintFloatNumber(double number)
{
    char        buffer[32];
    unsigned    len;
    
    len = snprintf(buffer, sizeof(buffer), "%lf", number);
    conn->Write(buffer, len);

    SetCommaNeeded(true);
}

void JSONSession::PrintBool(bool b)
{
    if (b)
        conn->Write("true", 4);
    else
        conn->Write("false", 5);

    SetCommaNeeded(true);
}

void JSONSession::PrintNull()
{
    conn->Write("null", 4);

    SetCommaNeeded(true);
}

void JSONSession::PrintObjectStart()
{
    conn->Print("{");
    depth++;
    SetCommaNeeded(false);
}

void JSONSession::PrintObjectEnd()
{
    conn->Print("}");
    depth--;
}

void JSONSession::PrintArrayStart()
{
    conn->Print("[");
    depth++;
    SetCommaNeeded(false);
}

void JSONSession::PrintArrayEnd()
{
    conn->Print("]");
    depth--;
}

void JSONSession::PrintColon()
{
    conn->Print(":");
}

void JSONSession::PrintComma()
{
    conn->Print(",");
}

void JSONSession::PrintPair(const char* s, unsigned slen, const char* v, unsigned vlen)
{
    if (IsCommaNeeded())
        PrintComma();

    PrintString(ReadBuffer((char *) s, slen));
    PrintColon();
    PrintString(ReadBuffer((char *) v, vlen));
    
    SetCommaNeeded(true);
}

bool JSONSession::IsCommaNeeded()
{
    uint64_t    mask;
    
    mask = 1 << depth;
    if ((depthComma & mask) == mask)
        return true;

    return false;
}

void JSONSession::SetCommaNeeded(bool needed)
{
    uint64_t    mask;
    
    assert(depth < sizeof(depthComma) * 8);
    
    mask = 1 << depth;
    if (needed)
        depthComma |= mask;
    else
        depthComma &= ~mask;
}