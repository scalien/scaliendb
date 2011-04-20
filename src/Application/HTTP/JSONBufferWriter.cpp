#include "JSONBufferWriter.h"

#include <stdio.h>

void JSONBufferWriter::Init(Buffer* output_)
{
    jsonCallback.SetLength(0);
    depth = 0;
    depthComma = 0;
    output = output_;
}

void JSONBufferWriter::SetCallbackPrefix(const ReadBuffer& jsonCallback_)
{
    jsonCallback = jsonCallback_;
}

void JSONBufferWriter::Start()
{
    if (jsonCallback.GetLength())
    {
        output->Append(jsonCallback.GetBuffer(), jsonCallback.GetLength());
        output->Append("(");
    }
    
    PrintObjectStart();
}

void JSONBufferWriter::End()
{
    PrintObjectEnd();
    if (jsonCallback.GetLength())
        output->Append(")");
}

void JSONBufferWriter::PrintStatus(const char* status, const char* type_)
{
    Start();
    
    output->Append("{\"status\":\"");
    output->Append(status);
    if (type_)
    {
        output->Append("\",\"type\":\"");
        output->Append(type_);
    }
    output->Append("\"}");

    End();
}

void JSONBufferWriter::PrintString(const ReadBuffer& str)
{
    const char* s;

    s = str.GetBuffer();
    output->Append("\"", 1);
    for (unsigned i = 0; i < str.GetLength(); i++)
    {
        if (s[i] == '"')
            output->Append("\\", 1);
        output->Append(s + i, 1);
    }
    output->Append("\"", 1);

    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintNumber(int64_t number)
{
    char        buffer[32];
    unsigned    len;
    
    len = snprintf(buffer, sizeof(buffer), "%" PRIi64, number);
    output->Append(buffer, len);

    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintFloatNumber(double number)
{
    char        buffer[32];
    unsigned    len;
    
    len = snprintf(buffer, sizeof(buffer), "%lf", number);
    output->Append(buffer, len);

    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintBool(bool b)
{
    if (b)
        output->Append("true", 4);
    else
        output->Append("false", 5);

    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintNull()
{
    output->Append("null", 4);

    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintObjectStart()
{
    if (IsCommaNeeded())
        PrintComma();
        
    output->Append("{");
    depth++;
    SetCommaNeeded(false);
}

void JSONBufferWriter::PrintObjectEnd()
{
    if (depth > 0)
    {
        output->Append("}");
        depth--;
    }
    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintArrayStart()
{
    if (IsCommaNeeded())
        PrintComma();

    output->Append("[");
    depth++;
    SetCommaNeeded(false);
}

void JSONBufferWriter::PrintArrayEnd()
{
    output->Append("]");
    depth--;
    SetCommaNeeded(true);
}

void JSONBufferWriter::PrintColon()
{
    output->Append(":");
    SetCommaNeeded(false);
}

void JSONBufferWriter::PrintComma()
{
    output->Append(",");
}

void JSONBufferWriter::PrintPair(const char* s, unsigned slen, const char* v, unsigned vlen)
{
    if (IsCommaNeeded())
        PrintComma();

    PrintString(ReadBuffer((char *) s, slen));
    PrintColon();
    PrintString(ReadBuffer((char *) v, vlen));
    
    SetCommaNeeded(true);
}

bool JSONBufferWriter::IsCommaNeeded()
{
    uint64_t    mask;

    // Since the info if you need a comma at a given depth is encoded into an uint64_t as a bitmask
    // there can be only 64 depth of the JSON object, but is is reasonable.
    mask = (uint64_t) 1 <<  depth;
    if ((depthComma & mask) == mask)
        return true;

    return false;
}

void JSONBufferWriter::SetCommaNeeded(bool needed)
{
    uint64_t    mask;
    
    ASSERT(depth < sizeof(depthComma) * 8);
    
    mask = (uint64_t) 1 << depth;
    if (needed)
        depthComma |= mask;
    else
        depthComma &= ~mask;
}