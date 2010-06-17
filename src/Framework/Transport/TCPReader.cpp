#include "TCPReader.h"
#include "System/Common.h"

/* class TCPReaderConn */

void TCPReaderConn::SetServer(TCPReader* reader_)
{
	reader = reader_;
}
	
void TCPReaderConn::OnMessageRead(const ByteString& message)
{
	reader->SetMessage(message);
	
	Call(reader->onRead);
}
	
void TCPReaderConn::OnClose()
{
	Log_Trace();
	
	Close();
	
	reader->OnConnectionClose(this);

	delete this;
}


/* class TransportTCPReader */

bool TCPReader::Init(int port)
{
	running = true;
	return TCPServer<TCPReader, TCPReaderConn>::Init(port);
}

void TCPReader::SetOnRead(const Callable& onRead_)
{
	onRead = onRead_;
}

void TCPReader::SetMessage(const ByteString& msg_)
{
	msg = msg_;
}

void TCPReader::GetMessage(ByteString& msg_)
{
	msg_ = msg;
}

bool TCPReader::IsActive()
{
	return running;
}

void TCPReader::Stop()
{
	Log_Trace();
	
	TCPReaderConn** it;

	running = false;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Stop();
		if (running)
			break; // user called Continue(), stop looping
	}
}

void TCPReader::Continue()
{
	Log_Trace();

	TCPReaderConn** it;
	
	running = true;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Continue();
		if (!running)
			break; // user called Stop(), stop looping
	}
}

void TCPReader::InitConn(TCPReaderConn* conn)
{
	conn->SetServer(this);
}

void TCPReader::OnConnectionClose(TCPReaderConn* conn)
{
	assert(conns.Remove(conn));
}

