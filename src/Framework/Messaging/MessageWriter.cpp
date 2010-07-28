#include "MessageWriter.h"
#include "System/Events/EventLoop.h"

#define CONNECT_TIMEOUT		2000

bool MessageWriter::Init(Endpoint &endpoint_)
{
	endpoint = endpoint_;
	TCPConnection::Connect(endpoint, CONNECT_TIMEOUT);
	return true;
}

void MessageWriter::Write(const Buffer& prefix, const Message& msg)
{
	ASSERT_FAIL(); // TODO: priority buffers can interleave! XZCXZCZXCZCZCZCZZXCZXCZ
	
	unsigned len;
	Buffer* head;
	Buffer* buffer;
	
	head = writer->AcquireBuffer();
	buffer = writer->AcquireBuffer();
	
	if (!msg.Write(*buffer))
		ASSERT_FAIL();
	len = prefix.GetLength() + 1 + buffer->GetLength();
	head->Writef("%u:%B:", len, &prefix);
	
	writer->Write(head);
	writer->Write(buffer);
	writer->Flush();
}

void MessageWriter::WritePriority(const Buffer& prefix, const Message& msg)
{
	unsigned len;
	Buffer* head;
	Buffer* buffer;
	
	head = writer->AcquireBuffer();
	buffer = writer->AcquireBuffer();
	
	if (!msg.Write(*buffer))
		ASSERT_FAIL();
	len = prefix.GetLength() + 1 + buffer->GetLength();
	head->Writef("%u:%B:", len, &prefix);
	
	Log_Trace("%.*s%.*s", P(head), P(buffer));
	
	writer->WritePriority(head);
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
	readBuffer.Clear();
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
