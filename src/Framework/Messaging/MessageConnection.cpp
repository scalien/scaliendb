#include "MessageConnection.h"
#include "MessageTransport.h"

MessageConnection::MessageConnection()
{
	resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
	resumeRead.SetDelay(1);
	node = NULL;
}

void MessageConnection::SetTransport(MessageTransport* transport_)
{
	Log_Trace();
	
	transport = transport_;
}

void MessageConnection::WritePriority(const Buffer& msg)
{
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	
	buffer->Writef("%u:", msg.GetLength());
	buffer->Append(msg);
	
	writer->WritePriority(buffer);
	writer->Flush();
}

void MessageConnection::Write(const Buffer& msg)
{
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	
	buffer->Writef("%u:", msg.GetLength());
	buffer->Append(msg);
	
	writer->Write(buffer);
	writer->Flush();
}

void MessageConnection::Write(const Buffer& prefix, const Message& msg)
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

void MessageConnection::WritePriority(const Buffer& prefix, const Message& msg)
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

void MessageConnection::OnClose()
{
	Log_Trace();
	
	if (connectTimeout.IsActive())
		return;
	
	Close();	
	transport->OnClose(this);
}

void MessageConnection::OnRead()
{
	bool			yield;
	unsigned		pos, msglength, nread, msgbegin, msgend, required;
	uint64_t		start;
	Stopwatch		sw;
	ReadBuffer		msg;

	sw.Start();
	
	Log_Trace("Read buffer: %.*s", P(tcpread.buffer));
	
	tcpread.requested = IO_READ_ANY;
	pos = 0;
	start = Now();
	yield = false;

	while(true)
	{
		msglength = strntouint64(tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos, &nread);
		
		if (msglength > tcpread.buffer->GetSize() - numlen(msglength) - 1)
		{
			Log_Trace();
			required = msglength + numlen(msglength) + 1;
			if (required > 10*MB)
			{
				Log_Trace();
				OnClose();
				return;
			}
			tcpread.buffer->Allocate(required);
			break;
		}
		
		if (nread == 0 || (unsigned) tcpread.buffer->GetLength() - pos <= nread)
			break;
			
		if (tcpread.buffer->GetCharAt(pos + nread) != ':')
		{
			Log_Trace("Message protocol error");
			OnClose();
			return;
		}
	
		msgbegin = pos + nread + 1;
		msgend = pos + nread + 1 + msglength;
		
		if ((unsigned) tcpread.buffer->GetLength() < msgend)
		{
			tcpread.requested = msgend - pos;
			break;
		}

		msg.SetBuffer(tcpread.buffer->GetBuffer() + msgbegin);
		msg.SetLength(msglength);
		transport->OnRead(this, msg);
		
		pos = msgend;
		
		// if the user called Close() in OnMessageRead()
		if (state != CONNECTED)
			return;
		
		if (tcpread.buffer->GetLength() == msgend)
			break;

		if (Now() - start > MESSAGING_YIELD_TIME)
		{
			// let other code run every YIELD_TIME msec
			yield = true;
			if (resumeRead.IsActive())
				ASSERT_FAIL();
			EventLoop::Add(&resumeRead);
			break;
		}
	}
	
	if (pos > 0)
	{
		memmove(tcpread.buffer->GetBuffer(), tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos);
		tcpread.buffer->Shorten(pos);
	}
	
	if (state == CONNECTED
	 && !tcpread.active && !yield)
		IOProcessor::Add(&tcpread);

	sw.Stop();
	Log_Trace("time spent in OnRead(): %ld", sw.Elapsed());
}

void MessageConnection::OnResumeRead()
{
	Log_Trace();

	OnRead();
}

void MessageConnection::Close()
{
	EventLoop::Remove(&resumeRead);
	TCPConnection::Close();	
}

void MessageConnection::Connect(const Endpoint& endpoint_)
{
	Log_Trace();
	
	endpoint = endpoint_;
	
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::OnConnect()
{
	TCPConnection::OnConnect();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	AsyncRead();
	
	transport->OnOutgoingConnection(this);
}

void MessageConnection::OnConnectTimeout()
{
	TCPConnection::OnConnectTimeout();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	Close();
	
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

Node* MessageConnection::GetNode()
{
	return node;
}

void MessageConnection::SetNode(Node* node_)
{
	node = node_;
}
