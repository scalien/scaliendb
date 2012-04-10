/*
 *  SafeFormatting.h
 *  ScalienDB
 *
 *  Created by Attila Gazso on 12/27/11.
 *  Copyright 2011 __MyCompanyName__. All rights reserved.
 *
 */

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
        CONST_CHAR_STAR,
        INT,
        UNSIGNED,
        LONG,
        UNSIGNED_LONG,
        INT64,
        UINT64,
        FLOAT,
        DOUBLE,
        BUFFER,
        READBUFFER
    };
};

class FormatStack
{
public:
    FormatStack(size_t size);

    void   Add(char c);
    void   Add(char* s);
    void   Add(const char* s);
    void   Add(int32_t i);
    void   Add(uint32_t u);
    void   Add(int64_t i);
    void   Add(uint64_t u);    
    void   Add(float f);
    void   Add(double d);
    void   Add(const Buffer& buffer);
    void   Add(const ReadBuffer& readBuffer);    
};

int SFWrite(char* buffer, size_t size, const char* fmt, const FormatStack& formatStack);

template <typename A0>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0)
{
    FormatStack formatStack(1);
    
    formatStack.Add(arg0);

    return SFWrite(buffer, size, fmt, formatStack);
}

template <typename A0, typename A1>
int SFWritef(char* buffer, size_t size, const char* fmt, A0 arg0, A1 arg1)
{
    FormatStack formatStack(2);
    
    formatStack.Add(arg0);
    formatStack.Add(arg1);

    return SFWrite(buffer, size, fmt, formatStack);
}



#endif
