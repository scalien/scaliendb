#include "TCPWriteQueue.h"
#include "TCPConnection.h"

TCPWriteQueue::TCPWriteQueue(TCPConnection* conn_, BufferPool* pool_)
{
	conn = conn_;
	if (pool_ == NULL)
		pool = DEFAULT_BUFFERPOOL;
	else
		pool = pool_;	
	writing = false;
}

TCPWriteQueue::~TCPWriteQueue()
{
	OnClose();
}

Buffer* TCPWriteQueue::AcquireBuffer(unsigned size)
{
	return pool->Acquire(size);
}

void TCPWriteQueue::ReleaseBuffer(Buffer* buffer)
{
	return pool->Release(buffer);
}

void TCPWriteQueue::Write(Buffer* buffer)
{
	Write(buffer->GetBuffer(), buffer->GetLength());
}

void TCPWriteQueue::Write(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.Enqueue(buffer);	
}

void TCPWriteQueue::WritePriority(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.EnqueuePriority(buffer);	
}

void TCPWriteQueue::WritePriority(Buffer* buffer)
{
	WritePriority(buffer->GetBuffer(), buffer->GetLength());
}

void TCPWriteQueue::WritePooled(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}

	queue.Enqueue(buffer);
}

void TCPWriteQueue::WritePooledPriority(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}
	
	queue.EnqueuePriority(buffer);
}

void TCPWriteQueue::Flush()
{
	conn->OnWritePending();
}

Buffer* TCPWriteQueue::GetNext()
{
	assert(writing = false);
	
	if (queue.GetLength() == 0)
		return NULL;
	
	writing = true;
	return queue.Head();
}

void TCPWriteQueue::OnNextWritten()
{
	assert(writing = true);
	writing = false;

	pool->Release(queue.Dequeue());
	if (queue.GetLength() > 0)
		conn->OnWritePending();	
}

void TCPWriteQueue::OnClose()
{
	while (queue.GetLength() > 0)
		pool->Release(queue.Dequeue());
}
	
unsigned TCPWriteQueue::BytesQueued()
{
	unsigned bytes;
	Buffer* buffer;
	
	bytes = 0;

	for (buffer = queue.Head(); buffer != NULL; buffer = queue.Next(buffer))
		bytes += buffer->GetLength();

	return bytes;
}
