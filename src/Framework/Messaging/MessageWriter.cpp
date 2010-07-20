#include "MessageWriter.h"
#include "System/Events/EventLoop.h"

#define CONNECT_TIMEOUT		2000


bool MessageWriter::Init(Endpoint &endpoint_)
{
	endpoint = endpoint_;
	TCPConnection::Connect(endpoint, CONNECT_TIMEOUT);
	return true;
}

void MessageWriter::Write(const Message& msg)
{
	Buffer* prefix;
	Buffer* buffer;
	
	prefix = writer->AcquireBuffer();
	buffer = writer->AcquireBuffer();
	
	msg.Write(buffer);
	prefix->Writef("%d:", buffer->GetLength());
	
	writer->Write(prefix);
	writer->Write(buffer);
	writer->Flush();
}

void MessageWriter::WritePriority(const Message& msg)
{
	Buffer* prefix;
	Buffer* buffer;
	
	prefix = writer->AcquireBuffer();
	buffer = writer->AcquireBuffer();
	
	msg.Write(buffer);
	prefix->Writef("%d:", buffer->GetLength());
	
	writer->WritePriority(prefix);
	writer->WritePriority(buffer);
	writer->Flush();
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
