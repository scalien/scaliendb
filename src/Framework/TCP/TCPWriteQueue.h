#ifndef TCPWRITEQUEUE
#define TCPWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Buffers/DynArray.h"
#include "System/Containers/Queue.h"

/*
===============================================================================

 TCPWriteQueue

===============================================================================
*/

class TCPWriteQueue : public TCPWriteProxy
{
protected:
	typedef DynArray<> Buffer;
	typedef Queue<Buffer, &Buffer::next> BufferQueue;

public:
	TCPWriteQueue(TCPConn* conn);
	virtual ~TCPWriteQueue();
	
	virtual void			Write(ByteString& bs);
	virtual void			Write(const char* buffer, unsigned length);
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
