#include "TCPPriorityWriteQueue.h"
#include "TCPConn.h"

TCPPriorityWriteQueue::TCPPriorityWriteQueue(TCPConn* conn_)
{
	conn = conn_;
	writing = false;
}

TCPPriorityWriteQueue::~TCPPriorityWriteQueue()
{
	Buffer* buf;

	while ((buf = queue.Dequeue()) != NULL)
		delete buf;
}

void TCPPriorityWriteQueue::Write(ByteString& bs)
{
	Write(bs.Buffer(), bs.Length());
}

void TCPPriorityWriteQueue::Write(const char* buffer, unsigned length)
{
	Buffer* buf;

	if (!buffer || length == 0)
		return;
	
	if (queue.Length() == 0 || (queue.Length() == 1 && writing) || buf->Remaining() < length)
	{
		buf = new Buffer;
		queue.Enqueue(buf);
	}
	else
	{
		buf = queue.Tail();
	}
	buf->Append(buffer, length);
}

void TCPPriorityWriteQueue::WritePriority(ByteString& bs)
{
	WritePriority(bs.Buffer(), bs.Length());
}

void TCPPriorityWriteQueue::WritePriority(const char* buffer, unsigned length)
{
	Buffer* buf;

	if (!buffer || length == 0)
		return;
	
	buf = new Buffer;
	queue.EnqueuePriority(buf);
	buf->Append(buffer, length);
}

void TCPPriorityWriteQueue::Flush()
{
	conn->OnWritePending();
}

ByteString TCPPriorityWriteQueue::GetNext()
{
	assert(writing = false);
	
	if (queue.Length() == 0)
		return ByteString();
	
	writing = true;
	return queue.Head()->ToByteString();
}

void TCPPriorityWriteQueue::OnNextWritten()
{
	assert(writing = true);
	writing = false;
	
	if (queue.Length() == 1)
	{
		queue.Head()->Rewind();
		Log_Trace("not posting write");
	}
	else
	{
		delete queue.Dequeue();
		conn->OnWritePending();
	}
}

void TCPPriorityWriteQueue::OnClose()
{
	// leave on buffer in queue if there are any, and rewind it

	while (queue.Length() > 1)
		delete queue.Dequeue();
	
	if (queue.Length() == 1)
		queue.Head()->Rewind();
}
	
unsigned TCPPriorityWriteQueue::BytesQueued()
{
	unsigned bytes;
	Buffer* buf;
	
	bytes = 0;

	for (buf = queue.Head(); buf != NULL; buf = queue.Next(buf))
		bytes += buf->Length();

	return bytes;
}
