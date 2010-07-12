#ifndef TCPWRITEPROXY_H
#define TCPWRITEPROXY_H

#include "System/Buffers/ByteString.h"

class TCPConn; // forward

/*
===============================================================================

 TCPWriteProxy ADT

===============================================================================
*/

class TCPWriteProxy
{
public:
	~TCPWriteProxy() {}

	virtual void			Write(ByteString& bs)						= 0;
	virtual void			Write(const char* buffer, unsigned length)	= 0;
	
	virtual ByteString		GetNext()									= 0;
	virtual void			OnNextWritten()								= 0;
	virtual void			OnClose()									= 0;
};

#endif
