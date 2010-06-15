#include "TransportTCPReader.h"
#include "System/Common.h"

/* class TransportTCPConn */

void TransportTCPConn::SetServer(TransportTCPReader* reader_)
{
	reader = reader_;
}
	
void TransportTCPConn::OnMessageRead(const ByteString& message)
{
	reader->SetMessage(message);
	
	Call(reader->onRead);
}
	
void TransportTCPConn::OnClose()
{
	Log_Trace();
	
	Close();
	
	reader->OnConnectionClose(this);

	delete this;
}


/* class TransportTCPReader */

bool TransportTCPReader::Init(int port)
{
	running = true;
	return TCPServer<TransportTCPReader, TransportTCPConn>::Init(port);
}

void TransportTCPReader::SetOnRead(Callable& onRead_)
{
	onRead = onRead_;
}

void TransportTCPReader::SetMessage(const ByteString& msg_)
{
	msg = msg_;
}

void TransportTCPReader::GetMessage(ByteString& msg_)
{
	msg_ = msg;
}

bool TransportTCPReader::IsActive()
{
	return running;
}

void TransportTCPReader::Stop()
{
	Log_Trace();
	
	TransportTCPConn** it;

	running = false;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Stop();
		if (running)
			break; // user called Continue(), stop looping
	}
}

void TransportTCPReader::Continue()
{
	Log_Trace();

	TransportTCPConn** it;
	
	running = true;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Continue();
		if (!running)
			break; // user called Stop(), stop looping
	}
}

void TransportTCPReader::InitConn(TransportTCPConn* conn)
{
	conn->SetServer(this);
}

void TransportTCPReader::OnConnectionClose(TransportTCPConn* conn)
{
	assert(conns.Remove(conn));
}

