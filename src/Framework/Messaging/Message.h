#ifndef MESSAGE_H
#define MESSAGE_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================

 Message

===============================================================================
*/

class Message
{
public:
    virtual ~Message() {};
    
    virtual bool        Read(ReadBuffer& buffer)        = 0;
    virtual bool        Write(Buffer& buffer)           = 0;
};

#endif
