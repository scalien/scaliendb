#ifndef TCPPRIORITYWRITEQUEUE
#define TCPPRIORITYWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Buffers/DynArray.h"
#include "System/Containers/PriorityQueue.h"

/*
===============================================================================

 TCPPriorityWriteQueue

===============================================================================
*/

class TCPPriorityWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef PriorityQueue<Buffer, &Buffer::next> BufferQueue;

public:
	TCPPriorityWriteQueue(TCPConn* conn);
	virtual ~TCPPriorityWriteQueue();
	
	virtual void			Write(ByteString& bs);
	virtual void			Write(const char* buffer, unsigned length);
	void					WritePriority(ByteString& bs);
	void					WritePriority(const char* buffer, unsigned length);
	void					Flush();

	virtual ByteString		GetNext();
	virtual void			OnNextWritten();	
	virtual void			OnClose();
	
	unsigned				BytesQueued();

protected:
	TCPConn*				conn;
	BufferQueue				queue;
	bool					writing;
};

#endif
