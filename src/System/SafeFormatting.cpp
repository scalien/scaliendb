#include "SafeFormatting.h"
#include "Formatting.h"
#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"

static int WriteArg(char* buffer, size_t size, TypedArg& arg)
{
    int     ret;
    
    ret = 0;
    
    switch (arg.type)
    {
    case TypedArg::CHAR:
        ret = Writef(buffer, size, "%c", *(char*) arg.pointer);
        break;
    case TypedArg::CHAR_STAR:
        ret = Writef(buffer, size, "%s", (const char*) arg.pointer);
        break;
    case TypedArg::INT32:
        ret = Writef(buffer, size, "%d", *(const int32_t*) arg.pointer);
        break;
    case TypedArg::UINT32:
        ret = Writef(buffer, size, "%u", *(const uint32_t*) arg.pointer);
        break;
    case TypedArg::INT64:
        ret = Writef(buffer, size, "%I", *(const int64_t*) arg.pointer);
        break;
    case TypedArg::UINT64:
        ret = Writef(buffer, size, "%U", *(const uint64_t*) arg.pointer);
        break;
    case TypedArg::FLOAT:
        ret = Writef(buffer, size, "%d", *(const float*) arg.pointer);
        break;
    case TypedArg::DOUBLE:
        ret = Writef(buffer, size, "%ld", *(const double*) arg.pointer);
        break;
    case TypedArg::BUFFER:
        ret = Writef(buffer, size, "%B", (const Buffer*) arg.pointer);
        break;
    case TypedArg::READBUFFER:
        ret = Writef(buffer, size, "%R", (const ReadBuffer*) arg.pointer);
        break;
    }
    
    return ret;
}

int SFWrite(char* buffer, size_t size, const char* fmt, const FormatStack& formatStack)
{
    const char*     f;
    size_t          pos;
    TypedArg        arg;
    int             ret;
    
    f = fmt;

    while (*f)
    {
        // start of a formatted argument
        if (*f == '{')
        {
            pos = f[1] - '0';
            arg = formatStack.args[pos];
            ret = WriteArg(buffer, size, arg);
            buffer += ret;
            size -= ret;
            
            f += 3;
            continue;
        }
        
        *buffer = *f;
        f += 1;
        
        buffer += 1;
        size -= 1;
    }
    
    return 0;
}
