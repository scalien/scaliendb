#ifndef TCPPRIORITYWRITEQUEUE
#define TCPPRIORITYWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Containers/PriorityQueueP.h"

/*
===============================================================================

 TCPPriorityWriteQueue

===============================================================================
*/

class TCPPriorityWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef PriorityQueueP<Buffer> BufferQueue;

public:
	TCPPriorityWriteQueue(TCPConn* conn);
	virtual ~TCPPriorityWriteQueue();
	
	virtual void			Write(Buffer& buffer);
	virtual void			Write(const char* buffer, unsigned length);
	void					WritePriority(Buffer& buffer);
	void					WritePriority(const char* buffer, unsigned length);
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
