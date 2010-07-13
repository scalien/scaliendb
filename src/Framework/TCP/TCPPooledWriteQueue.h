#ifndef TCPPOOLEDWRITEQUEUE
#define TCPPOOLEDWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Containers/QueueP.h"
#include "BufferPool.h"

/*
===============================================================================

 TCPPooledWriteQueue

===============================================================================
*/

class TCPPooledWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef QueueP<Buffer> BufferQueue;

public:
	TCPPooledWriteQueue(TCPConn* conn, BufferPool* pool = NULL);
	virtual ~TCPPooledWriteQueue();

	virtual void			Write(Buffer& buffer);
	virtual void			Write(const char* buffer, unsigned length);
	Buffer*					GetPooledBuffer(unsigned size = 0);
	void					WritePooled(Buffer* buffer);
	void					Flush();

	virtual Buffer*			GetNext();
	virtual void			OnNextWritten();	
	virtual void			OnClose();
	
	unsigned				BytesQueued();

protected:
	TCPConn*				conn;
	BufferQueue				queue;
	bool					writing;
	BufferPool*				pool;
};

#endif
