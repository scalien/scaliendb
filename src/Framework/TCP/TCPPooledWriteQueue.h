#ifndef TCPPOOLEDWRITEQUEUE
#define TCPPOOLEDWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Buffers/DynArray.h"
#include "System/Containers/Queue.h"
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
	typedef Queue<Buffer, &Buffer::next> BufferQueue;

public:
	TCPPooledWriteQueue(TCPConn* conn, BufferPool* pool = NULL);
	virtual ~TCPPooledWriteQueue();

	virtual void			Write(ByteString& bs);
	virtual void			Write(const char* buffer, unsigned length);
	Buffer*					GetPooledBuffer(unsigned size = 0);
	void					WritePooled(Buffer* buffer);
	void					Flush();

	virtual ByteString		GetNext();
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
