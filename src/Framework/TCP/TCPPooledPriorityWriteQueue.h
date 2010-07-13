#ifndef TCPPOOLEDPRIORITYWRITEQUEUE
#define TCPPOOLEDPRIORITYWRITEQUEUE

#include "TCPWriteProxy.h"
#include "System/Containers/PriorityQueueP.h"
#include "BufferPool.h"

/*
===============================================================================

 TCPPooledPriorityWriteQueue

===============================================================================
*/

class TCPPooledPriorityWriteQueue : public TCPWriteProxy
{
public:
	TCPPooledPriorityWriteQueue(TCPConn* conn, BufferPool* pool = NULL);
	virtual ~TCPPooledPriorityWriteQueue();
	
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
	TCPConn*					conn;
	PriorityQueueP<Buffer>		queue;
	BufferPool*					pool;
	bool						writing;
};

#endif
