#include "TCPPooledWriteQueue.h"
#include "TCPConn.h"
#include "BufferPool.h"

TCPPooledWriteQueue::TCPPooledWriteQueue(TCPConn* conn_, BufferPool* pool_)
{
	conn = conn_;
	if (pool_ == NULL)
		pool = DEFAULT_BUFFERPOOL;
	else
		pool = pool_;
	writing = false;
}

TCPPooledWriteQueue::~TCPPooledWriteQueue()
{
	OnClose();
}

void TCPPooledWriteQueue::Write(ByteString& bs)
{
	Write(bs.Buffer(), bs.Length());
}

void TCPPooledWriteQueue::Write(const char* buffer, unsigned length)
{
	Buffer* buf;

	if (!buffer || length == 0)
		return;
	
	buf = pool->Acquire(length);
	buf->Write(buffer, length);
	queue.Enqueue(buf);	
}

DynArray<>* TCPPooledWriteQueue::GetPooledBuffer(unsigned size)
{
	return pool->Acquire(size);
}

void TCPPooledWriteQueue::WritePooled(Buffer* buffer)
{
	if (!buffer || buffer->Length() == 0)
	{
		pool->Release(buffer);
		return;
	}

	queue.Enqueue(buffer);
}

void TCPPooledWriteQueue::Flush()
{
	conn->OnWritePending();
}

ByteString TCPPooledWriteQueue::GetNext()
{
	assert(writing = false);
	
	if (queue.Length() == 0)
		return ByteString();
	
	writing = true;
	return queue.Head()->ToByteString();
}

void TCPPooledWriteQueue::OnNextWritten()
{
	assert(writing = true);
	writing = false;

	pool->Release(queue.Dequeue());
	if (queue.Length() > 0)
		conn->OnWritePending();	
}

void TCPPooledWriteQueue::OnClose()
{
	while (queue.Length() > 0)
		pool->Release(queue.Dequeue());
}
	
unsigned TCPPooledWriteQueue::BytesQueued()
{
	unsigned bytes;
	Buffer* buf;
	
	bytes = 0;

	for (buf = queue.Head(); buf != NULL; buf = queue.Next(buf))
		bytes += buf->Length();

	return bytes;
}
