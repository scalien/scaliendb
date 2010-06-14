#include "TCPConn.h"
#include "System/Events/EventLoop.h"

TCPConn::TCPConn() :
 connectTimeout(&onConnectTimeout),
 onRead(this, &TCPConn::OnRead),
 onWrite(this, &TCPConn::OnWrite),
 onClose(this, &TCPConn::OnClose),
 onConnect(this, &TCPConn::OnConnect),
 onConnectTimeout(this, &TCPConn::OnConnectTimeout)
{
	state = DISCONNECTED;
	next = NULL;
}

TCPConn::~TCPConn()
{
	Buffer* buf;

	while ((buf = writeQueue.Dequeue()) != NULL)
		delete buf;
	
	Close();
}


void TCPConn::Init(bool startRead)
{
	Log_Trace();
	
	state = CONNECTED;
	
	readBuffer.Rewind();

	assert(tcpread.active == false);
	assert(tcpwrite.active == false);

	tcpwrite.fd = socket.fd;
	tcpwrite.onComplete = &onWrite;
	tcpwrite.onClose = &onClose;

	AsyncRead(startRead);
}

unsigned TCPConn::BytesQueued()
{
	unsigned bytes;
	Buffer* buf;
	
	bytes = 0;
	
	for (buf = writeQueue.Head(); buf != NULL; buf = buf->next)
		bytes += buf->Length();
	
	return bytes;
}


void TCPConn::Connect(Endpoint &endpoint_, unsigned timeout)
{
	Log_Trace("endpoint_ = %s", endpoint_.ToString());

	bool ret;

	if (state != DISCONNECTED)
		return;
		
	Init(false);
	state = CONNECTING;

	socket.Create(Socket::TCP);
	socket.SetNonblocking();
	ret = socket.Connect(endpoint_);
	
	tcpwrite.fd = socket.fd;
	tcpwrite.onComplete = &onConnect;
	// zero indicates for IOProcessor that we are waiting for connect event
	tcpwrite.data.Rewind();
	
	IOProcessor::Add(&tcpwrite);

	if (timeout > 0)
	{
		Log_Trace("starting timeout with %d", timeout);
		
		connectTimeout.SetDelay(timeout);
		EventLoop::Reset(&connectTimeout);
	}
}

void TCPConn::OnWrite()
{
	Log_Trace("Written %d bytes", tcpwrite.data.Length());
	Log_Trace("Written: %.*s", tcpwrite.data.Length(), tcpwrite.data.Buffer());

	Buffer* buf;
	
	buf = writeQueue.Dequeue();
	buf->Rewind();
	tcpwrite.data.Rewind();
	
	if (writeQueue.Size() == 0)
	{
		Log_Trace("not posting write");
		writeQueue.Enqueue(buf);
	}
	else
	{
		delete buf;
		WritePending();
	}
}

void TCPConn::OnConnect()
{
	Log_Trace();

	socket.SetNodelay();
	
	state = CONNECTED;
	tcpwrite.onComplete = &onWrite;
	
	EventLoop::Remove(&connectTimeout);
	WritePending();
}

void TCPConn::OnConnectTimeout()
{
	Log_Trace();
}

void TCPConn::AsyncRead(bool start)
{
	Log_Trace();
	
	tcpread.fd = socket.fd;
	tcpread.data.Wrap(readBuffer);
	tcpread.onComplete = &onRead;
	tcpread.onClose = &onClose;
	tcpread.requested = IO_READ_ANY;
	if (start)
		IOProcessor::Add(&tcpread);
	else
		Log_Trace("not posting read");
}

void TCPConn::Write(const char *data, int count, bool flush)
{
	//Log_Trace();
	
	if (state == DISCONNECTED)
		return;
	
	Buffer* buf;

	if (data && count > 0)
	{
		buf = writeQueue.Tail();

		if (!buf ||
			(tcpwrite.active && writeQueue.Size() == 1) || 
			(buf->Length() > 0 && buf->Remaining() < (unsigned)count))
		{
			buf = new Buffer;
			writeQueue.Enqueue(buf);
		}

		buf->Append(data, count);
	}

	if (flush)
		WritePending();
}

void TCPConn::WritePending()
{
//	Log_Trace();
	
	if (state == DISCONNECTED)
		return;

	
	Buffer* buf;
	
	if (tcpwrite.active)
		return;
	
	buf = writeQueue.Head();
	
	if (buf && buf->Length() > 0)
	{
		tcpwrite.data.Wrap(*buf);
		tcpwrite.transferred = 0;
		
		IOProcessor::Add(&tcpwrite);
	}	
}

void TCPConn::Close()
{
	Log_Trace();

	EventLoop::Remove(&connectTimeout);

	if (tcpread.active)
		IOProcessor::Remove(&tcpread);

	if (tcpwrite.active)
		IOProcessor::Remove(&tcpwrite);

	socket.Close();
	state = DISCONNECTED;
	
	// Discard unnecessary buffers if there are any.
	// Keep the last one, so that when the connection
	// is reused it isn't reallocated.
	while (writeQueue.Size() > 0)
	{
		Buffer* buf = writeQueue.Dequeue();
		if (writeQueue.Size() == 0)
		{
			buf->Rewind();
			writeQueue.Enqueue(buf);
			break;
		}
		else
			delete buf;
	}
	
	// TODO ? if the last buffer size exceeds some limit, 
	// reallocate it to a normal value
}
