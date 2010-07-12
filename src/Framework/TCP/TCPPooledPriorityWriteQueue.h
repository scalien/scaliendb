#ifndef TCPPOOLEDPRIORITYWRITEQUEUE
#define TCPPOOLEDPRIORITYWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Buffers/DynArray.h"
#include "System/Containers/PriorityQueue.h"
#include "BufferPool.h"

/*
===============================================================================

 TCPPooledPriorityWriteQueue

===============================================================================
*/

class TCPPooledPriorityWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef PriorityQueue<Buffer, &Buffer::next> BufferQueue;

public:
	TCPPooledPriorityWriteQueue(TCPConn* conn, BufferPool* pool = NULL);
	virtual ~TCPPooledPriorityWriteQueue();
	
	virtual void			Write(ByteString& bs);
	virtual void			Write(const char* buffer, unsigned length);
	void					WritePriority(ByteString& bs);
	void					WritePriority(const char* buffer, unsigned length);
	Buffer*					GetPooledBuffer(unsigned size = 0);
	void					WritePooled(Buffer* buffer);
	void					WritePooledPriority(Buffer* buffer);
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
