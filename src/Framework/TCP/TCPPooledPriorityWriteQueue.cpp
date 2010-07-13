#include "TCPPooledPriorityWriteQueue.h"
#include "TCPConn.h"

TCPPooledPriorityWriteQueue::TCPPooledPriorityWriteQueue(TCPConn* conn_, BufferPool* pool_)
{
	conn = conn_;
	if (pool_ == NULL)
		pool = DEFAULT_BUFFERPOOL;
	else
		pool = pool_;	
	writing = false;
}

TCPPooledPriorityWriteQueue::~TCPPooledPriorityWriteQueue()
{
	OnClose();
}

void TCPPooledPriorityWriteQueue::Write(Buffer* buffer)
{
	Write(buffer->GetBuffer(), buffer->GetLength());
}

void TCPPooledPriorityWriteQueue::Write(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.Enqueue(buffer);	
}

void TCPPooledPriorityWriteQueue::WritePriority(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.EnqueuePriority(buffer);	
}

Buffer* TCPPooledPriorityWriteQueue::GetPooledBuffer(unsigned size)
{
	return pool->Acquire(size);
}

void TCPPooledPriorityWriteQueue::WritePooled(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}

	queue.Enqueue(buffer);
}

void TCPPooledPriorityWriteQueue::WritePooledPriority(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}
	
	queue.EnqueuePriority(buffer);
}

void TCPPooledPriorityWriteQueue::Flush()
{
	conn->OnWritePending();
}

Buffer* TCPPooledPriorityWriteQueue::GetNext()
{
	assert(writing = false);
	
	if (queue.Length() == 0)
		return NULL;
	
	writing = true;
	return queue.Head();
}

void TCPPooledPriorityWriteQueue::OnNextWritten()
{
	assert(writing = true);
	writing = false;

	pool->Release(queue.Dequeue());
	if (queue.Length() > 0)
		conn->OnWritePending();	
}

void TCPPooledPriorityWriteQueue::OnClose()
{
	while (queue.Length() > 0)
		pool->Release(queue.Dequeue());
}
	
unsigned TCPPooledPriorityWriteQueue::BytesQueued()
{
	unsigned bytes;
	Buffer* buffer;
	
	bytes = 0;

	for (buffer = queue.Head(); buffer != NULL; buffer = queue.Next(buffer))
		bytes += buffer->GetLength();

	return bytes;
}
