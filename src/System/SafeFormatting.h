#ifndef SAFEFORMATTING_H
#define SAFEFORMATTING_H

#include "Platform.h"

class Buffer;
class ReadBuffer;

class TypedArg
{
public:
    enum Type
    {
        CHAR,
        CHAR_STAR,
        INT32,
        UINT32,
        INT64,
        UINT64,
        FLOAT,
        DOUBLE,
        BUFFER,
        READBUFFER
    };
    
    Type    type;
    void*   pointer;

    void    Set(char c);
    void    Set(char* s);
    void    Set(const char* s);
    void    Set(int32_t i);
    void    Set(uint32_t u);
    void    Set(int64_t i);
    void    Set(uint64_t u);    
    void    Set(float f);
    void    Set(double d);
    void    Set(const Buffer& buffer);
    void    Set(const ReadBuffer& readBuffer);
};

class FormatStack
{
public:
    FormatStack(TypedArg* args_, size_t numArgs_) : args(args_), numArgs(numArgs_) {}

    void     Add(size_t pos, const char& c);
    void     Add(size_t pos, const char* s);
    void     Add(size_t pos, const int32_t& i);
    void     Add(size_t pos, const uint32_t& u);
    void     Add(size_t pos, const int64_t& i);
    void     Add(size_t pos, const uint64_t& u);
    void     Add(size_t pos, const float& f);
    void     Add(size_t pos, const double& d);
    void     Add(size_t pos, const Buffer& buffer);
    void     Add(size_t pos, const ReadBuffer& readBuffer);
    
    TypedArg*   args;
    size_t      numArgs;
};

inline void FormatStack::Add(size_t pos, const char& c)
{
    args[pos].type = TypedArg::CHAR;
    args[pos].pointer = (void*) &c;
}

inline void FormatStack::Add(size_t pos, const char* s)
{
    args[pos].type = TypedArg::CHAR_STAR;
    args[pos].pointer = (void*) s;
}

inline void FormatStack::Add(size_t pos, const int32_t& i)
{
    args[pos].type = TypedArg::INT32;
    args[pos].pointer = (void*) &i;
}

inline void FormatStack::Add(size_t pos, const uint32_t& u)
{
    args[pos].type = TypedArg::UINT32;
    args[pos].pointer = (void*) &u;
}

inline void FormatStack::Add(size_t pos, const int64_t& i)
{
    args[pos].type = TypedArg::INT64;
    args[pos].pointer = (void*) &i;
}

inline void FormatStack::Add(size_t pos, const uint64_t& u)
{
    args[pos].type = TypedArg::UINT64;
    args[pos].pointer = (void*) &u;
}

inline void FormatStack::Add(size_t pos, const float& f)
{
    args[pos].type = TypedArg::FLOAT;
    args[pos].pointer = (void*) &f;
}

inline void FormatStack::Add(size_t pos, const double& d)
{
    args[pos].type = TypedArg::DOUBLE;
    args[pos].pointer = (void*) &d;
}

inline void FormatStack::Add(size_t pos, const Buffer& buffer)
{
    args[pos].type = TypedArg::BUFFER;
    args[pos].pointer = (void*) &buffer;
}

inline void FormatStack::Add(size_t pos, const ReadBuffer& readBuffer)
{
    args[pos].type = TypedArg::READBUFFER;
    args[pos].pointer = (void*) &readBuffer;
}

int SFWrite(char* buffer, size_t size, const char* fmt, const FormatStack& formatStack);

template <typename A0>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0)
{
    TypedArg    args[1];
    FormatStack formatStack(args, sizeof(args) / sizeof(TypedArg));
    
    formatStack.Add(0, arg0);

    return SFWrite(buffer, size, fmt, formatStack);
}

template <typename A0, typename A1>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0, A1 arg1)
{
    TypedArg    args[2];
    FormatStack formatStack(args, sizeof(args) / sizeof(TypedArg));
    
    formatStack.Add(0, arg0);
    formatStack.Add(1, arg1);

    return SFWrite(buffer, size, fmt, formatStack);
}

template <typename A0, typename A1, typename A2>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0, A1 arg1, A2 arg2)
{
    TypedArg    args[3];
    FormatStack formatStack(args, sizeof(args) / sizeof(TypedArg));
    
    formatStack.Add(0, arg0);
    formatStack.Add(1, arg1);
    formatStack.Add(2, arg2);

    return SFWrite(buffer, size, fmt, formatStack);
}

template <typename A0, typename A1, typename A2, typename A3>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0, A1 arg1, A2 arg2, A3 arg3)
{
    TypedArg    args[4];
    FormatStack formatStack(args, sizeof(args) / sizeof(TypedArg));
    
    formatStack.Add(0, arg0);
    formatStack.Add(1, arg1);
    formatStack.Add(2, arg2);
    formatStack.Add(3, arg3);

    return SFWrite(buffer, size, fmt, formatStack);
}

#endif
