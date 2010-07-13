#ifndef TCPWRITEQUEUE
#define TCPWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Containers/QueueP.h"

/*
===============================================================================

 TCPWriteQueue

===============================================================================
*/

class TCPWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef QueueP<Buffer> BufferQueue;

public:
	TCPWriteQueue(TCPConn* conn);
	virtual ~TCPWriteQueue();
	
	virtual void			Write(Buffer& buffer);
	virtual void			Write(const char* buffer, unsigned length);
	void					Flush();

	virtual Buffer*			GetNext();
	virtual void			OnNextWritten();	
	virtual void			OnClose();
	
	unsigned				BytesQueued();

protected:
	TCPConn*				conn;
	BufferQueue				queue;
	bool					writing;
};

#endif
