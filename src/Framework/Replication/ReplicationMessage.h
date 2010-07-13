#ifndef REPLICATIONMESSAGE_H
#define REPLICATIONMESSAGE_H

#include "System/Buffer.h"

/*
===============================================================================

 ReplicationMessage

===============================================================================
*/

class ReplicationMessage
{
public:

	virtual ~ReplicationMessage() {};
	
	virtual bool		Read(const Buffer* buffer)	= 0;
	virtual bool		Write(Buffer* buffer) const	= 0;
};

#endif
