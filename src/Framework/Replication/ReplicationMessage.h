#ifndef REPLICATIONMESSAGE_H
#define REPLICATIONMESSAGE_H

#include "System/Buffers/ByteString.h"
#include "System/Buffers/ByteBuffer.h"

/*
===============================================================================

 ReplicationMessage

===============================================================================
*/

class ReplicationMessage
{
public:

	virtual ~ReplicationMessage() {};
	
	virtual bool		Read(const ByteString& data)	= 0;
	virtual bool		Write(ByteBuffer& data) const	= 0;
};

#endif
