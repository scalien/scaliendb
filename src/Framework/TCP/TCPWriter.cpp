#include "TCPWriter.h"
#include "TCPConnection.h"

TCPWriter::TCPWriter(TCPConnection* conn_, BufferPool* pool_)
{
	conn = conn_;
	if (pool_ == NULL)
		pool = DEFAULT_BUFFERPOOL;
	else
		pool = pool_;	
}

TCPWriter::~TCPWriter()
{
	OnClose();
}

Buffer* TCPWriter::AcquireBuffer(unsigned size)
{
	return pool->Acquire(size);
}

void TCPWriter::ReleaseBuffer(Buffer* buffer)
{
	return pool->Release(buffer);
}

void TCPWriter::Write(Buffer* buffer)
{
	Write(buffer->GetBuffer(), buffer->GetLength());
}

void TCPWriter::Write(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.Enqueue(buffer);	
}

void TCPWriter::WritePriority(const char* p, unsigned length)
{
	Buffer* buffer;

	if (!p || length == 0)
		return;
	
	buffer = pool->Acquire(length);
	buffer->Write(p, length);
	queue.EnqueuePriority(buffer);	
}

void TCPWriter::WritePriority(Buffer* buffer)
{
	WritePriority(buffer->GetBuffer(), buffer->GetLength());
}

void TCPWriter::WritePooled(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}

	queue.Enqueue(buffer);
}

void TCPWriter::WritePooledPriority(Buffer* buffer)
{
	if (!buffer || buffer->GetLength() == 0)
	{
		pool->Release(buffer);
		return;
	}
	
	queue.EnqueuePriority(buffer);
}

void TCPWriter::Flush()
{
	Log_Trace();
	conn->OnWritePending();
}

Buffer* TCPWriter::GetNext()
{
	Log_Trace();

	if (queue.GetLength() == 0)
		return NULL;
	
	return queue.Head();
}

void TCPWriter::OnNextWritten()
{
	Log_Trace();
	
	pool->Release(queue.Dequeue());
	if (queue.GetLength() > 0)
		conn->OnWritePending();	
}

void TCPWriter::OnClose()
{
	while (queue.GetLength() > 0)
		pool->Release(queue.Dequeue());
}
	
unsigned TCPWriter::BytesQueued()
{
	unsigned bytes;
	Buffer* buffer;
	
	bytes = 0;

	for (buffer = queue.Head(); buffer != NULL; buffer = queue.Next(buffer))
		bytes += buffer->GetLength();

	return bytes;
}
