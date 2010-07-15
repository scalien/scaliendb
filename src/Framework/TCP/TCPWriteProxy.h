#ifndef TCPWRITEPROXY_H
#define TCPWRITEPROXY_H

#include "System/Buffer/Buffer.h"

/*
===============================================================================

 TCPWriteProxy ADT

===============================================================================
*/

class TCPWriteProxy
{
public:
	~TCPWriteProxy() {}

	virtual void			Write(Buffer* buffer)						= 0;
	virtual void			Write(const char* buffer, unsigned length)	= 0;

	// used by TCPConn:
	virtual Buffer*			GetNext()									= 0;
	virtual void			OnNextWritten()								= 0;
	virtual void			OnClose()									= 0;
};

#endif
