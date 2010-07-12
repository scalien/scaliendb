#include "TCPConn.h"
#include "System/Events/EventLoop.h"

TCPConn::TCPConn()
{
	state = DISCONNECTED;
	writeProxy = NULL;
	next = NULL;
	connectTimeout.SetCallable(MFUNC(TCPConn, OnConnectTimeout));
}

TCPConn::~TCPConn()
{	
	Close();
}

void TCPConn::Init(bool startRead)
{
	Log_Trace();
	
	assert(tcpread.active == false);
	assert(tcpwrite.active == false);

	readBuffer.Rewind();
	
	tcpwrite.SetFD(socket.fd);
	tcpwrite.SetOnComplete(MFUNC(TCPConn, OnRead));
	tcpwrite.SetOnClose(MFUNC(TCPConn, OnClose));

	AsyncRead(startRead);
}

void TCPConn::InitConnected(bool startRead)
{
	Init(startRead);
	state = CONNECTED;
}

void TCPConn::SetWriteProxy(TCPWriteProxy* writeProxy_)
{
	writeProxy = writeProxy_;
}

TCPWriteProxy* TCPConn::GetWriteProxy()
{
	return writeProxy;
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

	tcpwrite.SetFD(socket.fd);
	tcpwrite.SetOnComplete(MFUNC(TCPConn, OnConnect));
	tcpwrite.SetOnClose(MFUNC(TCPConn, OnClose));
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

	assert(writeProxy != NULL);
	writeProxy->OnNextWritten();
}

void TCPConn::OnConnect()
{
	Log_Trace();

	socket.SetNodelay();
	
	state = CONNECTED;
	tcpwrite.onComplete = MFUNC(TCPConn, OnWrite);
	
	EventLoop::Remove(&connectTimeout);
	OnWritePending();
}

void TCPConn::OnConnectTimeout()
{
	Log_Trace();
}

void TCPConn::AsyncRead(bool start)
{
	Log_Trace();
	
	tcpread.SetFD(socket.fd);
	tcpread.SetOnComplete(MFUNC(TCPConn, OnRead));
	tcpread.SetOnClose(MFUNC(TCPConn, OnClose));
	tcpread.Wrap(readBuffer);
	if (start)
		IOProcessor::Add(&tcpread);
	else
		Log_Trace("not posting read");
}

void TCPConn::OnWritePending()
{
	ByteString bs;

	if (state == DISCONNECTED || tcpwrite.active)
		return;

	assert(writeProxy != NULL);
	bs = writeProxy->GetNext();
	
	if (bs.Length() == 0)
		return;

	tcpwrite.Wrap(bs);
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
	
	if (writeProxy)
		writeProxy->OnClose();
}
