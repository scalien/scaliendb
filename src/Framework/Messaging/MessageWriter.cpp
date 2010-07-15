#include "MessageWriter.h"
#include "System/Events/EventLoop.h"

#define CONNECT_TIMEOUT		2000


bool MessageWriter::Init(Endpoint &endpoint_)
{
	endpoint = endpoint_;
	TCPConnection::Connect(endpoint, CONNECT_TIMEOUT);
	return true;
}

void MessageWriter::WritePrefix(Buffer* prefix, Buffer* message)
{
		prefix->Writef("%d:", message->GetLength());
}

void MessageWriter::Connect()
{
	Log_Trace();
	
	TCPConnection::Connect(endpoint, CONNECT_TIMEOUT);
}

void MessageWriter::OnConnect()
{
	TCPConnection::OnConnect();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	AsyncRead();
}

void MessageWriter::OnConnectTimeout()
{
	TCPConnection::OnConnectTimeout();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	Close();
	Connect();
}

void MessageWriter::OnRead()
{
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	// drop any data
	readBuffer.Rewind();
	AsyncRead();
}

void MessageWriter::OnClose()
{
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	if (!connectTimeout.IsActive())
	{
		Log_Trace("reset");
		EventLoop::Reset(&connectTimeout);
	}
}
