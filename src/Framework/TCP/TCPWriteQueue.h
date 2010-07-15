#ifndef TCPWRITEQUEUE_H
#define TCPWRITEQUEUE_H

#include "System/Containers/PriorityQueueP.h"
#include "System/Buffers/BufferPool.h"

class TCPConnection; // forward

/*
===============================================================================

 TCPWriteQueue

===============================================================================
*/

class TCPWriteQueue
{
public:
	TCPWriteQueue(TCPConnection* conn, BufferPool* pool = NULL);
	virtual ~TCPWriteQueue();
	
	Buffer*						GetPooledBuffer(unsigned size = 0);

	virtual void				Write(Buffer* buffer);
	virtual void				Write(const char* buffer, unsigned length);

	void						WritePriority(Buffer* buffer);
	void						WritePriority(const char* buffer, unsigned length);

	void						WritePooled(Buffer* buffer);
	void						WritePooledPriority(Buffer* buffer);
	void						Flush();

	virtual Buffer*				GetNext();
	virtual void				OnNextWritten();	
	virtual void				OnClose();
	
	unsigned					BytesQueued();

protected:
	TCPConnection*				conn;
	PriorityQueueP<Buffer>		queue;
	BufferPool*					pool;
	bool						writing;
};

#endif
