#include "TCPConn.h"
#include "System/Events/EventLoop.h"

TCPConn::TCPConn()
{
//	 connectTimeout(&onConnectTimeout),
//	 onRead(this, &TCPConn::OnRead),
//	 onWrite(this, &TCPConn::OnWrite),
//	 onClose(this, &TCPConn::OnClose),
//	 onConnect(this, &TCPConn::OnConnect),
//	 onConnectTimeout(this, &TCPConn::OnConnectTimeout)

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
	
	assert(tcpread.active == false);
	assert(tcpwrite.active == false);

	readBuffer.Rewind();
	
	tcpwrite.Set(socket.fd, MFUNC(TCPConn, OnRead), MFUNC(TCPConn, OnClose));

	AsyncRead(startRead);
}

void TCPConn::InitConnected(bool startRead)
{
	Init(startRead);
	state = CONNECTED;
}

unsigned TCPConn::BytesQueued()
{
	unsigned bytes;
	Buffer* buf;
	
	bytes = 0;

	for (buf = writeQueue.Head(); buf != NULL; buf = writeQueue.Next(buf))
		bytes += buf->Length();

	return bytes;
}

void TCPConn::Connect(Endpoint &endpoint, unsigned timeout)
{
	Log_Trace("endpoint_ = %s", endpoint.ToString());

	bool ret;

	if (state != DISCONNECTED)
		return;

	Init(false);
	state = CONNECTING;

	socket.Create(Socket::TCP);
	socket.SetNonblocking();
	ret = socket.Connect(endpoint);

	tcpwrite.Set(socket.fd, MFUNC(TCPConn, OnConnect), MFUNC(TCPConn, OnClose));	
	tcpwrite.AsyncConnect();
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
	
	if (writeQueue.Length() == 0)
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
	tcpwrite.onComplete = MFUNC(TCPConn, OnWrite);
	
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
	
	tcpread.Set(socket.fd, MFUNC(TCPConn, OnRead), MFUNC(TCPConn, OnClose));
	tcpread.Wrap(readBuffer);
	if (start)
		IOProcessor::Add(&tcpread);
	else
		Log_Trace("not posting read");
}

void TCPConn::Write(const char* buffer, unsigned length, bool flush)
{
	Buffer* buf;

	if (state == DISCONNECTED || !buffer || length == 0)
		return;
		
	buf = writeQueue.Tail();
	if (!buf ||
	 (tcpwrite.active && writeQueue.Length() == 1) || 
	 (buf->Length() > 0 && buf->Remaining() < length))
	{
		buf = new Buffer;
		writeQueue.Enqueue(buf);
	}
	buf->Append(buffer, length);

	if (flush)
		WritePending();
}

void TCPConn::WritePending()
{
	Buffer* buf;

	if (state == DISCONNECTED || tcpwrite.active)
		return;

	buf = writeQueue.Head();
	
	if (!buf || buf->Length() == 0)
		return;

	tcpwrite.Wrap(*buf);		
	IOProcessor::Add(&tcpwrite);
}

void TCPConn::Close()
{
	Log_Trace();

	EventLoop::Remove(&connectTimeout);

	IOProcessor::Remove(&tcpread);
	IOProcessor::Remove(&tcpwrite);

	socket.Close();
	state = DISCONNECTED;
	
	// Discard unnecessary buffers if there are any.
	// Keep the last one, so that when the connection
	// is reused it isn't reallocated.
	while (writeQueue.Length() > 0)
	{
		Buffer* buf = writeQueue.Dequeue();
		if (writeQueue.Length() == 0)
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
