#include "TCPWriter.h"
#include "System/Events/EventLoop.h"

#define CONNECT_TIMEOUT		2000


bool TCPWriter::Init(Endpoint &endpoint_)
{
	endpoint = endpoint_;
	TCPConn::Connect(endpoint, CONNECT_TIMEOUT);
	return true;
}

void TCPWriter::Write(Buffer* buffer)
{
	Log_Trace();

	char lbuf[20];
	int llen;
	
	if (state == CONNECTED)
	{
		llen = snwritef(lbuf, sizeof(lbuf), "%d:", buffer->GetLength());
		
		TCPConn::GetWriteProxy()->Write(lbuf, llen, false);
		TCPConn::GetWriteProxy()->Write(buffer);
	}
	else if (state == DISCONNECTED && !connectTimeout.IsActive())
		Connect();
}

void TCPWriter::Connect()
{
	Log_Trace();
	
	TCPConn::Connect(endpoint, CONNECT_TIMEOUT);
}

void TCPWriter::OnConnect()
{
	TCPConn::OnConnect();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	AsyncRead();
}

void TCPWriter::OnConnectTimeout()
{
	TCPConn::OnConnectTimeout();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	Close();
	Connect();
}

void TCPWriter::OnRead()
{
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	// drop any data
	readBuffer.Rewind();
	AsyncRead();
}

void TCPWriter::OnClose()
{
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	if (!connectTimeout.IsActive())
	{
		Log_Trace("reset");
		EventLoop::Reset(&connectTimeout);
	}
}
