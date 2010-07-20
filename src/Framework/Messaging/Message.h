#ifndef MESSAGE_H
#define MESSAGE_H

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
	
	virtual bool		Read(const ReadBuffer& buffer)	= 0;
	virtual bool		Write(Buffer& buffer) const		= 0;
};

#endif
