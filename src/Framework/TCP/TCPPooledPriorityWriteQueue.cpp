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

void TCPPooledPriorityWriteQueue::Write(ByteString& bs)
{
	Write(bs.Buffer(), bs.Length());
}

void TCPPooledPriorityWriteQueue::Write(const char* buffer, unsigned length)
{
	Buffer* buf;

	if (!buffer || length == 0)
		return;
	
	buf = pool->Acquire(length);
	buf->Write(buffer, length);
	queue.Enqueue(buf);	
}

void TCPPooledPriorityWriteQueue::WritePriority(ByteString& bs)
{
	WritePriority(bs.Buffer(), bs.Length());
}

void TCPPooledPriorityWriteQueue::WritePriority(const char* buffer, unsigned length)
{
	Buffer* buf;

	if (!buffer || length == 0)
		return;
	
	buf = pool->Acquire(length);
	buf->Write(buffer, length);
	queue.EnqueuePriority(buf);	
}

DynArray<>* TCPPooledPriorityWriteQueue::GetPooledBuffer(unsigned size)
{
	return pool->Acquire(size);
}

void TCPPooledPriorityWriteQueue::WritePooled(Buffer* buffer)
{
	if (!buffer || buffer->Length() == 0)
	{
		pool->Release(buffer);
		return;
	}

	queue.Enqueue(buffer);
}

void TCPPooledPriorityWriteQueue::WritePooledPriority(Buffer* buffer)
{
	if (!buffer || buffer->Length() == 0)
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

ByteString TCPPooledPriorityWriteQueue::GetNext()
{
	assert(writing = false);
	
	if (queue.Length() == 0)
		return ByteString();
	
	writing = true;
	return queue.Head()->ToByteString();
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
	Buffer* buf;
	
	bytes = 0;

	for (buf = queue.Head(); buf != NULL; buf = queue.Next(buf))
		bytes += buf->Length();

	return bytes;
}
